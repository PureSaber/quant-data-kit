from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quant_data_kit.adapters_v2 import (
    AdapterContext,
    AdapterInstrument,
    BinanceFixtureAdapter,
    CNNeutralFixtureAdapter,
    OKXFixtureAdapter,
    adapt_fixture_messages,
)
from quant_data_kit.adapters_v2.base import (
    BookSequenceNormalizer,
    event_identity,
    utc_from_milliseconds,
    utc_from_text,
)
from quant_data_kit.data_lake import StoragePolicy, write_normalized_events, write_raw_bytes
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import replay_l2

FIXTURES = Path(__file__).parent / "fixtures" / "providers"
SPOT = AdapterInstrument("CRYPTO:BTC-USDT:SPOT", price_scale=2, quantity_scale=3)
ETH_SPOT = AdapterInstrument("CRYPTO:ETH-USDT:SPOT", price_scale=2, quantity_scale=3)
PERP = AdapterInstrument("CRYPTO:BTC-USDT:PERP", price_scale=2, quantity_scale=3)
ETH_PERP = AdapterInstrument("CRYPTO:ETH-USDT:PERP", price_scale=2, quantity_scale=3)
TEST_POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


def load_messages(provider: str) -> list[dict]:
    return json.loads((FIXTURES / provider / "events.json").read_text(encoding="utf-8"))


def binance_adapter() -> BinanceFixtureAdapter:
    return BinanceFixtureAdapter(
        AdapterContext(
            provider="binance",
            venue="BINANCE",
            instruments={
                "BTCUSDT": SPOT,
                "ETHUSDT": ETH_SPOT,
                "BTCUSDT_PERP": PERP,
                "ETHUSDT_PERP": ETH_PERP,
            },
        )
    )


def okx_adapter() -> OKXFixtureAdapter:
    return OKXFixtureAdapter(
        AdapterContext(
            provider="okx",
            venue="OKX",
            instruments={
                "BTC-USDT": SPOT,
                "ETH-USDT": ETH_SPOT,
                "BTC-USDT-SWAP": PERP,
                "ETH-USDT-SWAP": ETH_PERP,
            },
        )
    )


def cn_adapter() -> CNNeutralFixtureAdapter:
    return CNNeutralFixtureAdapter(
        AdapterContext(
            provider="cn-fixture",
            venue="XSHG",
            instruments={
                "510300": AdapterInstrument("CN:XSHG:510300:ETF", price_scale=3, quantity_scale=0)
            },
            session_kind="exchange",
        )
    )


@pytest.mark.parametrize(
    ("provider", "adapter_factory", "expected_source", "expected_checkpoint"),
    [
        (
            "binance",
            binance_adapter,
            "binance",
            "3330859181cb817bcc90c347982bb4b90a085fa4db9fca225259cdfd8ff9139c",
        ),
        (
            "okx",
            okx_adapter,
            "okx",
            "7f371cef6144aafc191d055b4500f3ef6a38df65349c72f3b9dd418f156e9d00",
        ),
    ],
)
def test_crypto_fixture_adapters_cover_required_events_and_replay_l2(
    provider: str,
    adapter_factory,
    expected_source: str,
    expected_checkpoint: str,
) -> None:
    records = adapt_fixture_messages(adapter_factory(), load_messages(provider))
    assert {record["event_type"] for record in records} == {
        "trade",
        "quote",
        "book_snapshot",
        "book_delta",
        "funding_rate",
        "mark_price",
    }
    assert {record["source"] for record in records} == {expected_source}
    assert {record["instrument_id"] for record in records} >= {
        SPOT.instrument_id,
        PERP.instrument_id,
        ETH_PERP.instrument_id,
    }
    assert ETH_SPOT.instrument_id in {record["instrument_id"] for record in records}
    assert {
        record["event_type"]
        for record in records
        if record["instrument_id"] == ETH_PERP.instrument_id
    } == {"funding_rate", "mark_price"}
    assert all(record["event_time"].endswith("Z") for record in records)
    assert all(isinstance(record["sequence"], int) for record in records)
    l2 = [record for record in records if record["event_type"] in {"book_snapshot", "book_delta"}]
    result = replay_l2(l2)
    assert result.final_checkpoint.sequence > result.checkpoints[0].sequence
    assert len(result.final_checkpoint.state_sha256) == 64
    assert result.final_checkpoint.state_sha256 == expected_checkpoint


def test_binance_and_okx_map_to_same_stable_instrument_ids() -> None:
    binance = adapt_fixture_messages(binance_adapter(), load_messages("binance"))
    okx = adapt_fixture_messages(okx_adapter(), load_messages("okx"))
    binance_symbols = {record["instrument_id"] for record in binance}
    okx_symbols = {record["instrument_id"] for record in okx}
    assert {
        SPOT.instrument_id,
        ETH_SPOT.instrument_id,
        PERP.instrument_id,
        ETH_PERP.instrument_id,
    } <= (binance_symbols & okx_symbols)


def test_crypto_fixtures_enter_normalized_layer_without_quarantine(tmp_path: Path) -> None:
    for provider, venue, adapter in (
        ("binance", "BINANCE", binance_adapter()),
        ("okx", "OKX", okx_adapter()),
    ):
        records = adapt_fixture_messages(adapter, load_messages(provider))
        raw = write_raw_bytes(
            tmp_path,
            source=provider,
            request={"fixture": f"{provider}/events.json"},
            collected_at="2026-01-02T00:00:00Z",
            payload=(FIXTURES / provider / "events.json").read_bytes(),
            idempotency_key=f"{provider}-fixture-raw",
            policy=TEST_POLICY,
        )
        result = write_normalized_events(
            tmp_path,
            records,
            provider=provider,
            venue=venue,
            upstream_raw_references=[raw.reference()],
            policy=TEST_POLICY,
        )
        assert result.snapshot is not None
        assert result.accepted_rows == len(records)
        assert result.quarantined_rows == 0


def test_domestic_neutral_fixture_is_explicitly_not_market_data_certified() -> None:
    adapter = cn_adapter()
    assert adapter.certification_status == "fixture-certified-not-market-data-certified"
    records = adapt_fixture_messages(adapter, load_messages("cn_neutral"))
    assert {record["event_type"] for record in records} == {
        "book_snapshot",
        "book_delta",
    }
    assert {record["instrument_id"] for record in records} == {"CN:XSHG:510300:ETF"}
    replay = replay_l2(records)
    assert replay.final_checkpoint.sequence > records[0]["sequence"]
    assert (
        replay.final_checkpoint.state_sha256
        == "62510f268ac112a4ed18c572026e90d8b07179e101bd1f36b904963fa8255b09"
    )


def test_domestic_neutral_adapter_rejects_market_certification_claim() -> None:
    message = load_messages("cn_neutral")[0]
    message["certification_scope"] = "market-data-certified"
    with pytest.raises(ValidationError, match="fixture-only"):
        cn_adapter().adapt(message)


def test_provider_sequence_gap_is_rejected_before_v2_emission() -> None:
    messages = load_messages("binance")
    messages[4]["pu"] = 99
    with pytest.raises(ValidationError, match="gap|does not bridge"):
        adapt_fixture_messages(binance_adapter(), messages)


def test_binance_update_range_and_okx_trade_side_are_strict() -> None:
    binance_messages = load_messages("binance")
    binance_messages[4]["U"] = 102
    with pytest.raises(ValidationError, match="does not bridge"):
        adapt_fixture_messages(binance_adapter(), binance_messages)
    okx_messages = load_messages("okx")
    okx_messages[0]["side"] = "unknown"
    with pytest.raises(ValidationError, match="buy or sell"):
        adapt_fixture_messages(okx_adapter(), okx_messages)


def test_book_sequence_state_rolls_back_after_snapshot_and_delta_conversion_failure() -> None:
    binance = binance_adapter()
    snapshot = deepcopy(load_messages("binance")[3])
    broken_snapshot = deepcopy(snapshot)
    broken_snapshot["bids"][0][0] = "not-a-price"
    with pytest.raises(ValidationError, match="invalid"):
        binance.adapt(broken_snapshot)
    assert binance.adapt(snapshot)[0]["event_type"] == "book_snapshot"

    update = deepcopy(load_messages("binance")[4])
    broken_update = deepcopy(update)
    broken_update["a"][0][1] = "not-a-quantity"
    with pytest.raises(ValidationError, match="invalid"):
        binance.adapt(broken_update)
    assert len(binance.adapt(update)) == 2


def test_okx_books_checksum_fails_closed_and_corrected_messages_are_retryable() -> None:
    adapter = okx_adapter()
    snapshot = deepcopy(load_messages("okx")[3])
    bad_snapshot = deepcopy(snapshot)
    bad_snapshot["checksum"] += 1
    with pytest.raises(ValidationError, match="checksum mismatch"):
        adapter.adapt(bad_snapshot)
    assert adapter.adapt(snapshot)[0]["event_type"] == "book_snapshot"

    update = deepcopy(load_messages("okx")[4])
    bad_update = deepcopy(update)
    bad_update["checksum"] += 1
    with pytest.raises(ValidationError, match="checksum mismatch"):
        adapter.adapt(bad_update)
    assert len(adapter.adapt(update)) == 2

    assert "no books checksum" in BinanceFixtureAdapter.integrity_gate
    assert "CRC32" in OKXFixtureAdapter.integrity_gate


def test_fixture_index_hashes_are_exact() -> None:
    index = json.loads((FIXTURES / "index.json").read_text(encoding="utf-8"))
    assert index["certification_scope"] == "desensitized-fixture-only"
    assert "no books checksum" in index["provider_integrity"]["binance"]
    assert "CRC32" in index["provider_integrity"]["okx"]
    assert "not market-data-certified" in index["provider_integrity"]["cn_neutral"]
    assert set(index["l2_checkpoint_sha256"]) == {"binance", "okx", "cn_neutral"}
    for relative_path, expected_hash in index["sha256"].items():
        content = (FIXTURES / relative_path).read_bytes()
        assert hashlib.sha256(content).hexdigest() == expected_hash


def test_adapter_context_time_and_sequence_primitives_fail_closed() -> None:
    context = AdapterContext(provider="binance", venue="BINANCE", instruments={"BTCUSDT": SPOT})
    with pytest.raises(ValidationError, match="No stable instrument mapping"):
        context.instrument("MISSING")
    with pytest.raises(ValidationError, match="epoch milliseconds"):
        utc_from_milliseconds(True, "ts")
    with pytest.raises(ValidationError, match="epoch milliseconds"):
        utc_from_milliseconds("invalid", "ts")
    with pytest.raises(ValidationError, match="ISO-8601"):
        utc_from_text("invalid", "ts")
    with pytest.raises(ValidationError, match="must be UTC"):
        utc_from_text("2026-01-02T00:00:00+08:00", "ts")
    event_time = datetime(2026, 1, 2, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="precedes event_time"):
        event_identity(
            context,
            "BTCUSDT",
            event_time=event_time,
            received_at=event_time - timedelta(microseconds=1),
            event_id="invalid-time",
            sequence=None,
        )
    with pytest.raises(ValidationError, match="provider sequence"):
        event_identity(
            context,
            "BTCUSDT",
            event_time=event_time,
            received_at=event_time,
            event_id="invalid-sequence",
            sequence=-1,
        )

    sequences = BookSequenceNormalizer()
    with pytest.raises(ValidationError, match="before BookSnapshot"):
        sequences.delta(
            "BTCUSDT",
            provider_previous_sequence=1,
            provider_sequence=2,
            level_count=1,
        )
    sequences.snapshot("BTCUSDT", 10)
    with pytest.raises(ValidationError, match="did not advance"):
        sequences.snapshot("BTCUSDT", 10)
    with pytest.raises(ValidationError, match="did not advance"):
        sequences.delta(
            "BTCUSDT",
            provider_previous_sequence=10,
            provider_sequence=10,
            level_count=1,
        )
    with pytest.raises(ValidationError, match="level count"):
        sequences.delta(
            "BTCUSDT",
            provider_previous_sequence=10,
            provider_sequence=11,
            level_count=0,
        )


def test_adapter_constructors_and_unknown_message_kinds_are_strict() -> None:
    with pytest.raises(ValidationError, match="provider must be binance"):
        BinanceFixtureAdapter(
            AdapterContext(provider="okx", venue="OKX", instruments={"BTCUSDT": SPOT})
        )
    with pytest.raises(ValidationError, match="provider must be okx"):
        OKXFixtureAdapter(
            AdapterContext(provider="binance", venue="BINANCE", instruments={"BTC-USDT": SPOT})
        )
    with pytest.raises(ValidationError, match="exchange-session"):
        CNNeutralFixtureAdapter(
            AdapterContext(provider="cn-fixture", venue="XSHG", instruments={"510300": SPOT})
        )

    binance_unknown = deepcopy(load_messages("binance")[0])
    binance_unknown["e"] = "unknown"
    with pytest.raises(ValidationError, match="Unsupported Binance"):
        binance_adapter().adapt(binance_unknown)
    okx_unknown = deepcopy(load_messages("okx")[0])
    okx_unknown["channel"] = "unknown"
    with pytest.raises(ValidationError, match="Unsupported OKX"):
        okx_adapter().adapt(okx_unknown)
    cn_unknown = deepcopy(load_messages("cn_neutral")[0])
    cn_unknown["kind"] = "unknown"
    with pytest.raises(ValidationError, match="Unsupported domestic"):
        cn_adapter().adapt(cn_unknown)
