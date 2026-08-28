from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import quant_data_kit as qdk

FIXTURES = Path(__file__).parent / "fixtures" / "providers"
UTC = timezone.utc
TEST_POLICY = qdk.StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


def crypto_context(provider: str) -> qdk.AdapterContext:
    if provider == "binance":
        instruments = {
            "BTCUSDT": qdk.AdapterInstrument("CRYPTO:BTC-USDT:SPOT", 2, 3),
            "ETHUSDT": qdk.AdapterInstrument("CRYPTO:ETH-USDT:SPOT", 2, 3),
            "BTCUSDT_PERP": qdk.AdapterInstrument("CRYPTO:BTC-USDT:PERP", 2, 3),
        }
        venue = "BINANCE"
    else:
        instruments = {
            "BTC-USDT": qdk.AdapterInstrument("CRYPTO:BTC-USDT:SPOT", 2, 3),
            "ETH-USDT": qdk.AdapterInstrument("CRYPTO:ETH-USDT:SPOT", 2, 3),
            "BTC-USDT-SWAP": qdk.AdapterInstrument("CRYPTO:BTC-USDT:PERP", 2, 3),
        }
        venue = "OKX"
    return qdk.AdapterContext(provider=provider, venue=venue, instruments=instruments)


def test_public_m2_api_and_version_are_exposed() -> None:
    assert qdk.__version__ == "0.5.0"
    for name in (
        "write_raw_bytes",
        "write_normalized_events",
        "write_curated_bars",
        "DuckDBCatalog",
        "replay_l2",
        "BinanceFixtureAdapter",
        "OKXFixtureAdapter",
        "CNNeutralFixtureAdapter",
    ):
        assert getattr(qdk, name) is not None


def test_raw_to_normalized_duckdb_l2_curated_chain_is_replayable(tmp_path: Path) -> None:
    provider = "binance"
    fixture_path = FIXTURES / provider / "events.json"
    raw_bytes = fixture_path.read_bytes()
    raw = qdk.write_raw_bytes(
        tmp_path,
        source=provider,
        request={"fixture": "events.json", "desensitized": True},
        collected_at="2026-01-02T00:01:00Z",
        payload=raw_bytes,
        object_id="binance-m2-fixture",
        policy=TEST_POLICY,
    )
    manifest_path = next(tmp_path.rglob("binance-m2-fixture/manifest.json"))
    loaded_raw, loaded_bytes = qdk.load_raw_object(manifest_path)
    assert loaded_raw == raw
    assert loaded_bytes == raw_bytes

    adapter = qdk.BinanceFixtureAdapter(crypto_context(provider))
    records = qdk.adapt_fixture_messages(adapter, json.loads(loaded_bytes))
    normalized_result = qdk.write_normalized_events(
        tmp_path,
        records,
        provider=provider,
        venue="BINANCE",
        upstream_raw_ids=[raw.object_id],
    )
    assert normalized_result.snapshot is not None
    assert normalized_result.quarantined_rows == 0
    normalized = normalized_result.snapshot
    repeated = qdk.write_normalized_events(
        tmp_path,
        records,
        provider=provider,
        venue="BINANCE",
        upstream_raw_ids=[raw.object_id],
    )
    assert repeated.snapshot is not None
    assert repeated.snapshot.snapshot_id == normalized.snapshot_id

    with qdk.DuckDBCatalog(tmp_path).open_snapshot(normalized.snapshot_id) as catalog:
        assert catalog.query("SELECT count(*) AS rows FROM event_trade").to_pylist() == [
            {"rows": 2}
        ]

    l2_records = [
        record
        for record in records
        if record["event_type"] in {"book_snapshot", "book_delta"}
    ]
    first_replay = qdk.replay_l2(l2_records)
    assert qdk.replay_l2(l2_records).final_checkpoint.state_sha256 == (
        first_replay.final_checkpoint.state_sha256
    )

    trades = [record for record in records if record["event_type"] == "trade"]
    session_starts = {
        record["session_id"]: datetime(2026, 1, 2, tzinfo=UTC) for record in trades
    }
    bars = qdk.build_session_bars(
        trades,
        interval=timedelta(minutes=1),
        session_starts=session_starts,
        source="curated-binance",
    )
    curated = qdk.write_curated_bars(
        tmp_path,
        bars,
        dataset="crypto-session-bars-1m",
        revision_id="fixture-revision-1",
        recipe_version="session-bars-v1",
        lineage={"normalized_snapshot": normalized.snapshot_id},
    )
    assert curated.rows == 2
    assert qdk.load_curated_snapshot(
        tmp_path, "crypto-session-bars-1m", curated.snapshot_id
    ) == curated


def test_domestic_fixture_chain_remains_fixture_only(tmp_path: Path) -> None:
    messages = json.loads((FIXTURES / "cn_neutral" / "events.json").read_text(encoding="utf-8"))
    adapter = qdk.CNNeutralFixtureAdapter(
        qdk.AdapterContext(
            provider="cn-fixture",
            venue="XSHG",
            instruments={
                "510300": qdk.AdapterInstrument("CN:XSHG:510300:ETF", 3, 0)
            },
            session_kind="exchange",
        )
    )
    records = qdk.adapt_fixture_messages(adapter, messages)
    result = qdk.write_normalized_events(
        tmp_path,
        records,
        provider="cn-fixture",
        venue="XSHG",
        upstream_raw_ids=["domestic-desensitized-fixture"],
    )
    assert adapter.certification_status == "fixture-certified-not-market-data-certified"
    assert result.snapshot is not None
    assert result.quarantined_rows == 0
    assert qdk.replay_l2(records).final_checkpoint.sequence == 301_000_002
