from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from quant_data_kit.adapters_v2.base import BookSequenceNormalizer
from quant_data_kit.capture_v2.epoch import NormalizedEpochJournal
from quant_data_kit.capture_v2.models import (
    MarketKind,
    Provider,
    RawFrame,
    SegmentRotation,
    SymbolMappingResolver,
    default_crypto_l2_streams,
    default_symbol_mappings,
)
from quant_data_kit.capture_v2.storage import (
    CaptureStorageGuard,
    DiskCapacity,
    RawSegmentWriter,
    VolumeIdentity,
)
from quant_data_kit.capture_v2.synchronizers import (
    BinanceBookSynchronizer,
    OKXBookSynchronizer,
    ResyncRequired,
)
from quant_data_kit.data_lake import StoragePolicy, load_normalized_snapshot
from quant_data_kit.exceptions import ValidationError

UTC = timezone.utc
NOW = datetime(2026, 8, 29, 1, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)
POLICY = StoragePolicy(
    hot_quota_bytes=1024**3,
    minimum_free_bytes=1,
    minimum_free_fraction=0.000001,
)


def stream(provider: Provider, market: MarketKind, asset: str = "BTC"):
    return next(
        item
        for item in default_crypto_l2_streams()
        if item.provider is provider and item.market is market and asset in item.native_symbol
    )


def resolver():
    values = default_crypto_l2_streams()
    return SymbolMappingResolver(default_symbol_mappings(values))


def frame(config, payload: dict, index: int = 0, *, transport: str = "wss") -> RawFrame:
    received = NOW + timedelta(milliseconds=10 + index)
    url = config.websocket_url if transport == "wss" else config.rest_snapshot_url
    return RawFrame(
        frame_kind="market_data" if transport == "wss" else "rest_snapshot",
        provider=config.provider.value,
        stream_id=config.stream_id,
        connection_id="conn",
        subscription=config.channel,
        transport=transport,
        tls_url=str(url),
        received_at=received,
        observed_at=received,
        event_time=NOW,
        payload=json.dumps(payload, separators=(",", ":")).encode(),
        native_sequence={},
        collector_commit="abc",
    )


def binance_snapshot(config, last_id: int = 100) -> RawFrame:
    return frame(
        config,
        {"lastUpdateId": last_id, "bids": [["100", "2"]], "asks": [["101", "3"]]},
        transport="https",
    )


def binance_update(config, first: int, final: int, *, previous: int | None = None, **changes):
    payload = {
        "e": "depthUpdate",
        "E": NOW_MS,
        "T": NOW_MS,
        "s": config.native_symbol,
        "U": first,
        "u": final,
        "b": changes.get("bids", [["100", "2.5"]]),
        "a": changes.get("asks", []),
    }
    if previous is not None:
        payload["pu"] = previous
    return frame(config, payload, final)


def okx_message(config, action: str, seq: int, prev: int, *, bids=None, asks=None, checksum=0):
    return frame(
        config,
        {
            "arg": {"channel": "books", "instId": config.native_symbol},
            "action": action,
            "data": [
                {
                    "ts": str(NOW_MS),
                    "seqId": seq,
                    "prevSeqId": prev,
                    "checksum": checksum,
                    "bids": bids if bids is not None else [],
                    "asks": asks if asks is not None else [],
                }
            ],
        },
        seq,
    )


def storage(tmp_path: Path):
    hot, archive = tmp_path / "hot", tmp_path / "archive"
    hot.mkdir()
    archive.mkdir()

    def identity(path: Path) -> VolumeIdentity:
        value = "archive" if path == archive else "hot"
        return VolumeIdentity(value, (value,))

    guard = CaptureStorageGuard(
        hot,
        archive,
        policy=POLICY,
        archive_reserve_bytes=1,
        volume_identity=identity,
        capacity_probe=lambda _path: DiskCapacity(10**9, 9 * 10**8),
        hot_size_probe=lambda _path: 0,
    )
    return hot, archive, guard


def test_binance_futures_snapshot_bridge_pu_chain_and_absent_delete_audit() -> None:
    config = stream(Provider.BINANCE, MarketKind.USDT_PERPETUAL)
    sync = BinanceBookSynchronizer(config, resolver())
    snapshot = sync.admit_snapshot(binance_snapshot(config))
    assert snapshot.snapshot_admitted and snapshot.records[0]["event_type"] == "book_snapshot"

    stale = sync.admit_update(binance_update(config, 90, 100, previous=90))
    assert not stale.records and stale.observations[0].event == "stale_buffered_update"
    bridged = sync.admit_update(
        binance_update(
            config,
            99,
            101,
            previous=99,
            bids=[["99", "0"], ["100", "2.5"]],
        )
    )
    assert sync.live
    assert len(bridged.records) == 1
    assert bridged.records[0]["action"] == "upsert"
    assert bridged.observations[0].event == "absent_level_delete"

    no_change = sync.admit_update(
        binance_update(config, 102, 102, previous=101, bids=[["98", "0"]])
    )
    assert not no_change.records and no_change.observations[0].event == "absent_level_delete"
    accepted = sync.admit_update(binance_update(config, 103, 103, previous=102))
    assert accepted.records
    duplicate = sync.admit_update(binance_update(config, 103, 103, previous=102))
    assert duplicate.observations[0].event == "duplicate_or_old_update"


def test_binance_spot_continuity_snapshot_lag_and_usdm_pu_fail_closed() -> None:
    spot = stream(Provider.BINANCE, MarketKind.SPOT)
    spot_sync = BinanceBookSynchronizer(spot, resolver())
    spot_sync.admit_snapshot(binance_snapshot(spot))
    spot_sync.admit_update(binance_update(spot, 100, 101))
    assert spot_sync.admit_update(binance_update(spot, 101, 102)).records
    with pytest.raises(ResyncRequired, match="discontinuous"):
        spot_sync.admit_update(binance_update(spot, 104, 104))

    stale_snapshot = BinanceBookSynchronizer(spot, resolver())
    stale_snapshot.admit_snapshot(binance_snapshot(spot))
    with pytest.raises(ResyncRequired, match="did not bridge"):
        stale_snapshot.admit_update(binance_update(spot, 101, 102))

    future = stream(Provider.BINANCE, MarketKind.USDT_PERPETUAL)
    future_sync = BinanceBookSynchronizer(future, resolver())
    future_sync.admit_snapshot(binance_snapshot(future))
    future_sync.admit_update(binance_update(future, 100, 101, previous=100))
    with pytest.raises(ResyncRequired, match="pu chain"):
        future_sync.admit_update(binance_update(future, 102, 102, previous=99))
    missing_pu = binance_update(future, 102, 102)
    with pytest.raises(ResyncRequired, match="missing pu"):
        future_sync.admit_update(missing_pu)


@pytest.mark.parametrize(
    "payload,message",
    [
        ({"lastUpdateId": "bad", "bids": [], "asks": []}, "snapshot is malformed"),
        ({"lastUpdateId": 1, "bids": [["101", "1"]], "asks": [["100", "1"]]}, "crossed"),
    ],
)
def test_binance_snapshot_malformed_and_crossed(payload: dict, message: str) -> None:
    config = stream(Provider.BINANCE, MarketKind.SPOT)
    with pytest.raises(ResyncRequired, match=message):
        BinanceBookSynchronizer(config, resolver()).admit_snapshot(
            frame(config, payload, transport="https")
        )


def test_binance_unexpected_messages_ranges_mapping_and_update_before_snapshot() -> None:
    config = stream(Provider.BINANCE, MarketKind.SPOT)
    sync = BinanceBookSynchronizer(config, resolver())
    with pytest.raises(ResyncRequired, match="before REST"):
        sync.admit_update(binance_update(config, 1, 2))
    sync.admit_snapshot(binance_snapshot(config))
    wrong_event = binance_update(config, 100, 101)
    wrong_payload = json.loads(wrong_event.payload)
    wrong_payload["e"] = "trade"
    with pytest.raises(ResyncRequired, match="unexpected"):
        sync.admit_update(frame(config, wrong_payload))
    with pytest.raises(ResyncRequired, match="reversed"):
        sync.admit_update(binance_update(config, 102, 101))

    bad_mapping = SymbolMappingResolver(())
    mapped = BinanceBookSynchronizer(config, bad_mapping)
    with pytest.raises(ResyncRequired, match="exactly one"):
        mapped.admit_snapshot(binance_snapshot(config))


def test_okx_snapshot_heartbeat_updates_deprecated_checksum_and_maintenance_reset() -> None:
    config = stream(Provider.OKX, MarketKind.SPOT)
    sync = OKXBookSynchronizer(config, resolver())
    control = sync.admit_message(
        frame(
            config,
            {"event": "subscribe", "arg": {"channel": "books"}, "code": "0", "msg": ""},
        )
    )
    assert control.observations[0].event == "subscription_control"
    snapshot = sync.admit_message(
        okx_message(
            config,
            "snapshot",
            10,
            -1,
            bids=[["100", "2"]],
            asks=[["101", "3"]],
            checksum=123,
        )
    )
    assert snapshot.snapshot_admitted and snapshot.records
    assert snapshot.observations[0].event == "deprecated_checksum_ignored"
    heartbeat = sync.admit_message(okx_message(config, "update", 10, 10))
    assert not heartbeat.records and heartbeat.observations[-1].event == "book_heartbeat"
    update = sync.admit_message(
        okx_message(
            config,
            "update",
            11,
            10,
            bids=[["99", "0"], ["100", "2.5"]],
        )
    )
    assert len(update.records) == 1
    assert update.observations[-1].event == "absent_level_delete"
    with pytest.raises(ResyncRequired, match="maintenance"):
        sync.admit_message(okx_message(config, "update", 9, 11))
    assert sync.live is False
    with pytest.raises(ResyncRequired, match="fresh books snapshot"):
        sync.admit_message(okx_message(config, "update", 12, 11))


def test_okx_gap_heartbeat_levels_errors_and_envelope_guards() -> None:
    config = stream(Provider.OKX, MarketKind.USDT_PERPETUAL)
    sync = OKXBookSynchronizer(config, resolver())
    with pytest.raises(ResyncRequired, match="subscription error"):
        sync.admit_message(frame(config, {"event": "error", "code": "60012", "msg": "bad"}))
    with pytest.raises(ResyncRequired, match="envelope"):
        sync.admit_message(frame(config, {"arg": {}, "data": {}}))
    wrong = okx_message(config, "snapshot", 10, -1, bids=[["100", "1"]], asks=[["101", "1"]])
    value = json.loads(wrong.payload)
    value["arg"]["instId"] = "WRONG"
    with pytest.raises(ResyncRequired, match="unexpected"):
        sync.admit_message(frame(config, value))
    sync.admit_message(
        okx_message(config, "snapshot", 10, -1, bids=[["100", "1"]], asks=[["101", "1"]])
    )
    with pytest.raises(ResyncRequired, match="prevSeqId"):
        sync.admit_message(okx_message(config, "update", 12, 9))
    with pytest.raises(ResyncRequired, match="heartbeat contained"):
        sync.admit_message(okx_message(config, "update", 10, 10, bids=[["100", "2"]]))
    unsupported = json.loads(okx_message(config, "update", 11, 10).payload)
    unsupported["action"] = "partial"
    with pytest.raises(ResyncRequired, match="unsupported"):
        sync.admit_message(frame(config, unsupported))


def test_binance_wire_and_book_negative_branches_are_fail_closed() -> None:
    config = stream(Provider.BINANCE, MarketKind.SPOT)
    with pytest.raises(ValidationError, match="BinanceBookSynchronizer"):
        BinanceBookSynchronizer(stream(Provider.OKX, MarketKind.SPOT), resolver())

    top_level_list = replace(frame(config, {}), payload=b"[]")
    with pytest.raises(ResyncRequired, match="JSON object"):
        BinanceBookSynchronizer(config, resolver()).admit_snapshot(top_level_list)

    wrapped = replace(
        binance_snapshot(config),
        payload=json.dumps(
            {
                "data": {
                    "lastUpdateId": 100,
                    "bids": [["100", "2"]],
                    "asks": [["101", "3"]],
                }
            }
        ).encode(),
    )
    assert BinanceBookSynchronizer(config, resolver()).admit_snapshot(wrapped).records

    invalid_snapshots = (
        ({"lastUpdateId": 100, "bids": [["NaN", "1"]], "asks": []}, "positive price"),
        ({"lastUpdateId": 100, "bids": [["100", "-1"]], "asks": []}, "non-negative"),
        ({"lastUpdateId": 100, "bids": [["100"]], "asks": []}, "price/quantity"),
        ({"lastUpdateId": 100, "bids": [["100", "0"]], "asks": []}, "zero quantity"),
        (
            {
                "lastUpdateId": 100,
                "bids": [["100", "1"], ["100", "2"]],
                "asks": [],
            },
            "duplicate prices",
        ),
    )
    for payload, message in invalid_snapshots:
        with pytest.raises(ResyncRequired, match=message):
            BinanceBookSynchronizer(config, resolver()).admit_snapshot(
                frame(config, payload, transport="https")
            )

    sync = BinanceBookSynchronizer(config, resolver())
    sync.admit_snapshot(binance_snapshot(config))
    sync.admit_update(binance_update(config, 100, 101))

    missing_time = json.loads(binance_update(config, 102, 102).payload)
    missing_time.pop("T")
    missing_time.pop("E")
    with pytest.raises(ResyncRequired, match="event time is missing"):
        sync.admit_update(frame(config, missing_time))
    with pytest.raises(ResyncRequired, match="price/quantity"):
        sync.admit_update(binance_update(config, 102, 102, bids=[["100"]]))
    with pytest.raises(ResyncRequired, match="duplicate prices"):
        sync.admit_update(binance_update(config, 102, 102, bids=[["100", "2"], ["100", "3"]]))
    with pytest.raises(ResyncRequired, match="cross or lock"):
        sync.admit_update(binance_update(config, 102, 102, bids=[["102", "1"]]))
    deleted = sync.admit_update(binance_update(config, 102, 102, bids=[["100", "0"]]))
    assert deleted.records and not deleted.observations


def test_mapping_disagreement_and_okx_non_object_item_require_resync() -> None:
    class WrongResolver:
        def resolve(self, *_args, **_kwargs) -> str:
            return "CRYPTO:WRONG:INSTRUMENT"

    binance = stream(Provider.BINANCE, MarketKind.SPOT)
    with pytest.raises(ResyncRequired, match="disagrees"):
        BinanceBookSynchronizer(binance, WrongResolver()).admit_snapshot(binance_snapshot(binance))

    okx = stream(Provider.OKX, MarketKind.SPOT)
    with pytest.raises(ValidationError, match="OKXBookSynchronizer"):
        OKXBookSynchronizer(binance, resolver())
    malformed_item = frame(
        okx,
        {
            "arg": {"channel": "books", "instId": okx.native_symbol},
            "action": "snapshot",
            "data": [1],
        },
    )
    with pytest.raises(ResyncRequired, match="data item must be an object"):
        OKXBookSynchronizer(okx, resolver()).admit_message(malformed_item)
    with pytest.raises(ResyncRequired, match="disagrees"):
        OKXBookSynchronizer(okx, WrongResolver()).admit_message(
            okx_message(okx, "snapshot", 10, -1)
        )


def test_sequence_normalizer_no_change_branch_is_strict() -> None:
    sequences = BookSequenceNormalizer()
    with pytest.raises(ValidationError, match="before BookSnapshot"):
        sequences.advance_without_levels(
            "BTCUSDT", provider_previous_sequence=1, provider_sequence=2
        )
    sequences.snapshot("BTCUSDT", 10)
    with pytest.raises(ValidationError, match="no-change gap"):
        sequences.advance_without_levels(
            "BTCUSDT", provider_previous_sequence=9, provider_sequence=11
        )
    with pytest.raises(ValidationError, match="did not advance"):
        sequences.advance_without_levels(
            "BTCUSDT", provider_previous_sequence=10, provider_sequence=10
        )
    sequences.advance_without_levels("BTCUSDT", provider_previous_sequence=10, provider_sequence=11)
    assert sequences.delta(
        "BTCUSDT",
        provider_previous_sequence=11,
        provider_sequence=12,
        level_count=1,
    )


def test_normalized_epoch_journal_publishes_complete_raw_lineage(tmp_path: Path) -> None:
    hot, _, guard = storage(tmp_path)
    config = stream(Provider.BINANCE, MarketKind.SPOT)
    writer = RawSegmentWriter(
        hot,
        config.stream_id,
        config.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=2),
        storage_guard=guard,
        policy=POLICY,
    )
    writer.append(frame(config, {"raw": 1}, 1))
    writer.append(frame(config, {"raw": 2}, 2))
    segment = writer.drain_completed()[0]

    sync = BinanceBookSynchronizer(config, resolver())
    records = list(sync.admit_snapshot(binance_snapshot(config)).records)
    records.extend(sync.admit_update(binance_update(config, 100, 101)).records)
    journal = NormalizedEpochJournal(
        hot,
        epoch_id="epoch-one",
        stream_id=config.stream_id,
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=POLICY,
        max_part_rows=1,
    )
    journal.append(records)
    journal.record_segment(segment)
    receipt = journal.finalize(created_at=NOW)
    assert receipt.accepted_rows == len(records)
    assert receipt.quarantined_rows == 0
    assert receipt.raw_segments == 1
    assert len(receipt.journal_parts) == len(records)
    assert receipt.normalized_snapshot_id
    snapshot = load_normalized_snapshot(hot, receipt.normalized_snapshot_id)
    assert snapshot.rows == len(records)
    assert Path(receipt.receipt_path).is_file()


def test_epoch_empty_abort_duplicate_lineage_and_missing_raw_are_visible(tmp_path: Path) -> None:
    hot, _, guard = storage(tmp_path)
    config = stream(Provider.BINANCE, MarketKind.SPOT)
    empty = NormalizedEpochJournal(
        hot,
        epoch_id="empty",
        stream_id=config.stream_id,
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=POLICY,
    )
    empty_receipt = empty.finalize(created_at=NOW)
    assert empty_receipt.records == 0 and empty_receipt.normalized_snapshot_id is None
    with pytest.raises(ValidationError, match="closed"):
        empty.append([])

    aborted = NormalizedEpochJournal(
        hot,
        epoch_id="aborted",
        stream_id=config.stream_id,
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=POLICY,
    )
    assert aborted.abort_visible("injected gap").is_file()

    no_raw = NormalizedEpochJournal(
        hot,
        epoch_id="no-raw",
        stream_id=config.stream_id,
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=POLICY,
    )
    sync = BinanceBookSynchronizer(config, resolver())
    no_raw.append(sync.admit_snapshot(binance_snapshot(config)).records)
    with pytest.raises(ValidationError, match="require Raw"):
        no_raw.finalize(created_at=NOW)

    writer = RawSegmentWriter(
        hot,
        config.stream_id,
        config.provider.value,
        collector_commit="abc",
        rotation=SegmentRotation(max_messages=2),
        storage_guard=guard,
        policy=POLICY,
    )
    writer.append(frame(config, {"raw": 1}, 1))
    writer.append(frame(config, {"raw": 2}, 2))
    segment = writer.drain_completed()[0]
    duplicate = NormalizedEpochJournal(
        hot,
        epoch_id="duplicate",
        stream_id=config.stream_id,
        provider="binance",
        venue="BINANCE",
        storage_guard=guard,
        policy=POLICY,
    )
    duplicate.record_segment(segment)
    with pytest.raises(ValidationError, match="duplicate Raw"):
        duplicate.record_segment(segment)
    duplicate.abort_visible("test complete")


def test_epoch_configuration_closed_abort_and_part_integrity_guards(tmp_path: Path) -> None:
    hot, _, guard = storage(tmp_path)
    config = stream(Provider.BINANCE, MarketKind.SPOT)
    common = {
        "hot_root": hot,
        "stream_id": config.stream_id,
        "provider": "binance",
        "venue": "BINANCE",
        "storage_guard": guard,
        "policy": POLICY,
    }
    with pytest.raises(ValidationError, match="max_part_rows"):
        NormalizedEpochJournal(epoch_id="invalid-parts", max_part_rows=0, **common)

    closed = NormalizedEpochJournal(epoch_id="closed-abort", **common)
    closed.finalize(created_at=NOW)
    with pytest.raises(ValidationError, match="cannot abort"):
        closed.abort_visible("visible after close")

    tampered = NormalizedEpochJournal(epoch_id="tampered", max_part_rows=1, **common)
    tampered.append(({"event_type": "test", "value": 1},))
    part = tampered.root / tampered._parts[0].relative_path
    part.write_bytes(part.read_bytes() + b" ")
    with pytest.raises(ValidationError, match="journal part integrity changed"):
        tuple(tampered._iter_records())
    tampered.abort_visible("integrity failure")
