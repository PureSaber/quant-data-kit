from __future__ import annotations

import asyncio
import hashlib
import json
from concurrent.futures import Future
from types import SimpleNamespace

import pytest

from quant_data_kit.capture_v2 import epoch as epoch_module
from quant_data_kit.capture_v2.epoch import EpochPart, NormalizedEpochJournal
from quant_data_kit.capture_v2.models import MarketKind, Provider, default_crypto_l2_streams
from quant_data_kit.data_lake import StoragePolicy
from quant_data_kit.exceptions import ProviderError, ValidationError
from tools.benchmark_capture_v2 import (
    SCENARIOS,
    BenchmarkTiming,
    FixtureHttp,
    ProviderBatchingExecutor,
    _book_levels,
    _expected_normalized_rows,
    _messages,
)


def test_dense_burst_fixture_has_frozen_depth_and_expected_row_density() -> None:
    scenario = SCENARIOS["dense-burst"]
    assert scenario.delta_levels_per_side == 20
    assert scenario.binance_snapshot_levels_per_side == 1000
    assert scenario.okx_snapshot_levels_per_side == 400
    assert _expected_normalized_rows(500, scenario) == 159_688

    streams = default_crypto_l2_streams()
    binance = next(
        item
        for item in streams
        if item.provider is Provider.BINANCE and item.market is MarketKind.USDT_PERPETUAL
    )
    binance_update = json.loads(_messages(binance, 3, scenario)[0])
    assert len(binance_update["b"]) == len(binance_update["a"]) == 20
    assert len({item[0] for item in binance_update["b"]}) == 20
    assert len({item[0] for item in binance_update["a"]}) == 20

    okx = next(item for item in streams if item.provider is Provider.OKX)
    okx_messages = _messages(okx, 3, scenario)
    okx_snapshot = json.loads(okx_messages[1])["data"][0]
    okx_update = json.loads(okx_messages[2])["data"][0]
    assert len(okx_snapshot["bids"]) == len(okx_snapshot["asks"]) == 400
    assert len(okx_update["bids"]) == len(okx_update["asks"]) == 20

    snapshot = asyncio.run(
        FixtureHttp(streams, scenario).get(binance.rest_snapshot_url, timeout_seconds=1)
    )
    binance_snapshot = json.loads(snapshot.body)
    assert len(binance_snapshot["bids"]) == len(binance_snapshot["asks"]) == 1000
    assert binance_snapshot["symbol"] == binance.native_symbol


def test_sparse_fixture_is_retained_and_fixture_guards_fail_closed() -> None:
    sparse = SCENARIOS["sparse"]
    assert sparse.delta_levels_per_side == 1
    assert _expected_normalized_rows(500, sparse) == 7_992
    assert _book_levels("bid", 2, quantity_seed=1) == [
        ["100.00", "1"],
        ["99.99", "2"],
    ]
    with pytest.raises(ValueError, match="unsupported book side"):
        _book_levels("middle", 1, quantity_seed=1)

    http = FixtureHttp(default_crypto_l2_streams(), sparse)
    with pytest.raises(ProviderError, match="unexpected snapshot URL"):
        asyncio.run(http.get("https://example.invalid/depth", timeout_seconds=1))
    with pytest.raises(ProviderError, match="timeout must be positive"):
        asyncio.run(http.get("https://example.invalid/depth", timeout_seconds=0))


def test_epoch_group_is_fail_closed_and_preserves_per_journal_counts(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = StoragePolicy(minimum_free_bytes=1, minimum_free_fraction=0.000001)
    part = EpochPart("unused.ndjson", 2, "0" * 64, 0)
    job = (tmp_path, tmp_path, (part,), "binance", "BINANCE", (), policy)

    monkeypatch.setattr(epoch_module, "_iter_epoch_record_batches", lambda *_args: iter(()))

    def accepted(_root, batches, **_kwargs):
        assert list(batches) == []
        return SimpleNamespace(
            snapshot=SimpleNamespace(snapshot_id="snapshot-one"),
            accepted_rows=4,
            quarantined_rows=0,
        )

    monkeypatch.setattr(epoch_module, "write_normalized_batches", accepted)
    summaries = epoch_module._publish_epoch_group((job, job))
    assert [item.accepted_rows for item in summaries] == [2, 2]
    assert {item.snapshot_id for item in summaries} == {"snapshot-one"}

    with pytest.raises(ValidationError, match="must not be empty"):
        epoch_module._publish_epoch_group(())
    mismatched = (*job[:3], "okx", *job[4:])
    with pytest.raises(ValidationError, match="identity or policy"):
        epoch_module._publish_epoch_group((job, mismatched))

    monkeypatch.setattr(
        epoch_module,
        "write_normalized_batches",
        lambda *_args, **_kwargs: SimpleNamespace(
            snapshot=None, accepted_rows=3, quarantined_rows=0
        ),
    )
    with pytest.raises(ValidationError, match="row accounting"):
        epoch_module._publish_epoch_group((job, job))


def test_epoch_executor_and_journal_decode_negative_branches(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ImmediateExecutor:
        def submit(self, fn, *args):
            assert fn is epoch_module._publish_epoch_parts
            assert args[0] == tmp_path
            future = Future()
            future.set_result(epoch_module._NormalizationSummary("snapshot", 7, 0))
            return future

    journal = object.__new__(NormalizedEpochJournal)
    journal.hot_root = tmp_path
    journal.root = tmp_path
    journal._parts = []
    journal.provider = "binance"
    journal.venue = "BINANCE"
    journal._raw_references = []
    journal.policy = StoragePolicy(minimum_free_bytes=1, minimum_free_fraction=0.000001)
    journal.normalization_executor = ImmediateExecutor()
    assert journal._publish_normalized().accepted_rows == 7
    with pytest.raises(ValidationError, match="identity is missing"):
        NormalizedEpochJournal._record_batch(None, [])

    malformed = b"not-json\n"
    malformed_path = tmp_path / "malformed.ndjson"
    malformed_path.write_bytes(malformed)
    malformed_part = EpochPart(
        malformed_path.name,
        1,
        hashlib.sha256(malformed).hexdigest(),
        len(malformed),
    )
    with pytest.raises(ValidationError, match="line is malformed"):
        list(epoch_module._iter_epoch_records(tmp_path, (malformed_part,)))

    scalar = b"[]\n"
    scalar_path = tmp_path / "scalar.ndjson"
    scalar_path.write_bytes(scalar)
    scalar_part = EpochPart(
        scalar_path.name,
        1,
        hashlib.sha256(scalar).hexdigest(),
        len(scalar),
    )
    with pytest.raises(ValidationError, match="must be an object"):
        list(epoch_module._iter_epoch_records(tmp_path, (scalar_part,)))


def test_provider_batching_executor_propagates_results_and_failures() -> None:
    class ImmediateDelegate:
        def __init__(self, result=None, error: Exception | None = None) -> None:
            self.result = result
            self.error = error

        def submit(self, _fn, _group):
            future = Future()
            if self.error is not None:
                future.set_exception(self.error)
            else:
                future.set_result(self.result)
            return future

    arguments = (None, None, (), "binance", "BINANCE", (), StoragePolicy())
    summary = epoch_module._NormalizationSummary("snapshot", 1, 0)
    executor = ProviderBatchingExecutor(ImmediateDelegate((summary,)), flush_seconds=60)
    with pytest.raises(RuntimeError, match="only accepts"):
        executor.submit(lambda: None)
    future = executor.submit(epoch_module._publish_epoch_parts, *arguments)
    with pytest.raises(RuntimeError, match="not quiescent"):
        executor.assert_quiescent()
    executor.shutdown()
    executor.assert_quiescent()
    assert future.result() == summary
    executor._launch_locked("missing")

    failed = ProviderBatchingExecutor(
        ImmediateDelegate(error=RuntimeError("worker failed")), flush_seconds=60
    )
    failed_future = failed.submit(epoch_module._publish_epoch_parts, *arguments)
    with pytest.raises(RuntimeError, match="worker failed"):
        failed.shutdown()
    with pytest.raises(RuntimeError, match="worker failed"):
        failed_future.result()


def test_benchmark_timing_excludes_only_post_report_worker_teardown() -> None:
    timing = BenchmarkTiming(started_at=10.0)
    with pytest.raises(RuntimeError, match="has not completed"):
        _ = timing.workload_seconds
    with pytest.raises(RuntimeError, match="closed before workload"):
        timing.mark_pool_closed(11.0)
    with pytest.raises(RuntimeError, match="boundary is invalid"):
        timing.mark_workload_completed(9.0)

    timing.mark_workload_completed(12.5)
    with pytest.raises(RuntimeError, match="boundary is invalid"):
        timing.mark_workload_completed(13.0)
    with pytest.raises(RuntimeError, match="teardown has not completed"):
        _ = timing.worker_teardown_seconds
    timing.mark_pool_closed(15.0)
    assert timing.workload_seconds == 2.5
    assert timing.worker_teardown_seconds == 2.5
    assert timing.total_wall_seconds == 5.0
