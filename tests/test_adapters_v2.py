from __future__ import annotations

import hashlib
import json
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
from quant_data_kit.data_lake import write_normalized_events
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.l2_replay import replay_l2

FIXTURES = Path(__file__).parent / "fixtures" / "providers"
SPOT = AdapterInstrument("CRYPTO:BTC-USDT:SPOT", price_scale=2, quantity_scale=3)
ETH_SPOT = AdapterInstrument("CRYPTO:ETH-USDT:SPOT", price_scale=2, quantity_scale=3)
PERP = AdapterInstrument("CRYPTO:BTC-USDT:PERP", price_scale=2, quantity_scale=3)


def load_messages(provider: str) -> list[dict]:
    return json.loads((FIXTURES / provider / "events.json").read_text(encoding="utf-8"))


def binance_adapter() -> BinanceFixtureAdapter:
    return BinanceFixtureAdapter(
        AdapterContext(
            provider="binance",
            venue="BINANCE",
            instruments={"BTCUSDT": SPOT, "ETHUSDT": ETH_SPOT, "BTCUSDT_PERP": PERP},
        )
    )


def okx_adapter() -> OKXFixtureAdapter:
    return OKXFixtureAdapter(
        AdapterContext(
            provider="okx",
            venue="OKX",
            instruments={"BTC-USDT": SPOT, "ETH-USDT": ETH_SPOT, "BTC-USDT-SWAP": PERP},
        )
    )


def cn_adapter() -> CNNeutralFixtureAdapter:
    return CNNeutralFixtureAdapter(
        AdapterContext(
            provider="cn-fixture",
            venue="XSHG",
            instruments={
                "510300": AdapterInstrument(
                    "CN:XSHG:510300:ETF", price_scale=3, quantity_scale=0
                )
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
    assert {record["instrument_id"] for record in records} >= {SPOT.instrument_id, PERP.instrument_id}
    assert ETH_SPOT.instrument_id in {record["instrument_id"] for record in records}
    assert all(record["event_time"].endswith("Z") for record in records)
    l2 = [
        record
        for record in records
        if record["event_type"] in {"book_snapshot", "book_delta"}
    ]
    result = replay_l2(l2)
    assert result.final_checkpoint.sequence > result.checkpoints[0].sequence
    assert len(result.final_checkpoint.state_sha256) == 64
    assert result.final_checkpoint.state_sha256 == expected_checkpoint


def test_binance_and_okx_map_to_same_stable_instrument_ids() -> None:
    binance = adapt_fixture_messages(binance_adapter(), load_messages("binance"))
    okx = adapt_fixture_messages(okx_adapter(), load_messages("okx"))
    binance_symbols = {record["instrument_id"] for record in binance}
    okx_symbols = {record["instrument_id"] for record in okx}
    assert {SPOT.instrument_id, ETH_SPOT.instrument_id, PERP.instrument_id} <= (
        binance_symbols & okx_symbols
    )


def test_crypto_fixtures_enter_normalized_layer_without_quarantine(tmp_path: Path) -> None:
    for provider, venue, adapter in (
        ("binance", "BINANCE", binance_adapter()),
        ("okx", "OKX", okx_adapter()),
    ):
        records = adapt_fixture_messages(adapter, load_messages(provider))
        result = write_normalized_events(
            tmp_path,
            records,
            provider=provider,
            venue=venue,
            upstream_raw_ids=[f"{provider}-fixture-raw"],
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


def test_fixture_index_hashes_are_exact() -> None:
    index = json.loads((FIXTURES / "index.json").read_text(encoding="utf-8"))
    assert index["certification_scope"] == "desensitized-fixture-only"
    assert set(index["l2_checkpoint_sha256"]) == {"binance", "okx", "cn_neutral"}
    for relative_path, expected_hash in index["sha256"].items():
        content = (FIXTURES / relative_path).read_bytes()
        assert hashlib.sha256(content).hexdigest() == expected_hash
