"""Run the M7 synthetic L2 normalization performance and determinism gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date, datetime, timezone
from functools import wraps
from importlib.metadata import version
from pathlib import Path
from typing import Any

import psutil
import pyarrow as pa
import pyarrow.compute as pc
from typing_extensions import Self

from quant_data_kit import normalized_v3
from quant_data_kit.data_lake import (
    StoragePolicy,
    load_normalized_snapshot,
    write_normalized_batches,
    write_raw_bytes,
)
from quant_data_kit.schemas_v2 import (
    BOOK_DELTA_EVENT_SCHEMA_ID,
    BOOK_SNAPSHOT_EVENT_SCHEMA_ID,
    get_arrow_schema,
)

_PROVIDER = "binance"
_VENUE = "BINANCE"
_INSTRUMENT = "BTC-USDT-SPOT"
_SESSION = "binance-24x7-BTC-USDT-SPOT"
_TIMESTAMP = "2026-01-02T00:00:00Z"
_TRADING_DAY = "2026-01-02"
_TIMESTAMP_VALUE = datetime(2026, 1, 2, tzinfo=timezone.utc)
_TRADING_DAY_VALUE = date(2026, 1, 2)
_GIB = 1024**3


def synthetic_l2_events(rows: int) -> Iterator[dict[str, Any]]:
    """Yield one snapshot and sequential same-book deltas in constant memory."""
    if rows < 2:
        raise ValueError("rows must be at least 2")
    yield {
        "event_type": "book_snapshot",
        "event_id": "m7-snapshot-1",
        "instrument_id": _INSTRUMENT,
        "event_time": _TIMESTAMP,
        "received_at": _TIMESTAMP,
        "available_at": _TIMESTAMP,
        "source": _PROVIDER,
        "trading_day": _TRADING_DAY,
        "session_id": _SESSION,
        "sequence": 1,
        "bids": [
            {
                "price": {"units": 10_000_000, "scale": 2},
                "quantity": {"units": 100_000, "scale": 3},
                "order_count": 10,
            }
        ],
        "asks": [
            {
                "price": {"units": 10_000_100, "scale": 2},
                "quantity": {"units": 100_000, "scale": 3},
                "order_count": 10,
            }
        ],
    }
    for sequence in range(2, rows + 1):
        yield {
            "event_type": "book_delta",
            "event_id": f"m7-delta-{sequence}",
            "instrument_id": _INSTRUMENT,
            "event_time": _TIMESTAMP,
            "received_at": _TIMESTAMP,
            "available_at": _TIMESTAMP,
            "source": _PROVIDER,
            "trading_day": _TRADING_DAY,
            "session_id": _SESSION,
            "sequence": sequence,
            "side": "bid",
            "action": "upsert",
            "price": {"units": 10_000_000, "scale": 2},
            "quantity": {"units": (sequence & 1023) + 1, "scale": 3},
            "previous_sequence": sequence - 1,
        }


def _repeat(value: Any, data_type: pa.DataType, length: int) -> pa.Array:
    return pa.repeat(pa.scalar(value, type=data_type), length)


def synthetic_l2_batches(rows: int, batch_rows: int) -> Iterator[pa.RecordBatch]:
    """Yield schema-exact normalized Arrow batches without retaining the dataset."""
    if rows < 2:
        raise ValueError("rows must be at least 2")
    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")
    snapshot_schema = get_arrow_schema(BOOK_SNAPSHOT_EVENT_SCHEMA_ID)
    snapshot_record = next(synthetic_l2_events(2))
    snapshot_record["event_time"] = _TIMESTAMP_VALUE
    snapshot_record["received_at"] = _TIMESTAMP_VALUE
    snapshot_record["available_at"] = _TIMESTAMP_VALUE
    snapshot_record["trading_day"] = _TRADING_DAY_VALUE
    yield pa.RecordBatch.from_pylist([snapshot_record], schema=snapshot_schema)

    schema = get_arrow_schema(BOOK_DELTA_EVENT_SCHEMA_ID)
    timestamp_type = schema.field("event_time").type
    date_type = schema.field("trading_day").type
    price_type = schema.field("price").type
    quantity_type = schema.field("quantity").type
    for start in range(2, rows + 1, batch_rows):
        stop = min(start + batch_rows, rows + 1)
        length = stop - start
        sequence = pa.array(range(start, stop), type=pa.int64())
        previous = pc.subtract(sequence, pa.scalar(1, pa.int64()))
        event_id = pc.binary_join_element_wise(
            _repeat("m7-delta-", pa.string(), length),
            pc.cast(sequence, pa.string()),
            "",
        )
        price = pa.StructArray.from_arrays(
            [
                _repeat(10_000_000, pa.int64(), length),
                _repeat(2, pa.int16(), length),
            ],
            fields=list(price_type),
        )
        quantity = pa.StructArray.from_arrays(
            [
                pc.add(
                    pc.bit_wise_and(sequence, pa.scalar(1023, pa.int64())),
                    pa.scalar(1, pa.int64()),
                ),
                _repeat(3, pa.int16(), length),
            ],
            fields=list(quantity_type),
        )
        arrays = {
            "event_type": _repeat("book_delta", pa.string(), length),
            "event_id": event_id,
            "instrument_id": _repeat(_INSTRUMENT, pa.string(), length),
            "event_time": _repeat(_TIMESTAMP_VALUE, timestamp_type, length),
            "received_at": _repeat(_TIMESTAMP_VALUE, timestamp_type, length),
            "available_at": _repeat(_TIMESTAMP_VALUE, timestamp_type, length),
            "source": _repeat(_PROVIDER, pa.string(), length),
            "trading_day": _repeat(_TRADING_DAY_VALUE, date_type, length),
            "session_id": _repeat(_SESSION, pa.string(), length),
            "sequence": sequence,
            "side": _repeat("bid", pa.string(), length),
            "action": _repeat("upsert", pa.string(), length),
            "price": price,
            "quantity": quantity,
            "previous_sequence": previous,
        }
        yield pa.RecordBatch.from_arrays(
            [arrays[field.name] for field in schema],
            schema=schema,
        )


class _PeakRssSampler:
    def __init__(self, interval_seconds: float = 0.01) -> None:
        self._interval_seconds = interval_seconds
        self._process = psutil.Process()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self.peak_bytes = self._process.memory_info().rss

    def _sample(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self.peak_bytes = max(self.peak_bytes, self._process.memory_info().rss)

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join()
        self.peak_bytes = max(self.peak_bytes, self._process.memory_info().rss)


@contextmanager
def _profile_storage_stages() -> Iterator[dict[str, dict[str, float | int]]]:
    function_names = (
        "_duplicate_event_ids",
        "_partition_manifests",
        "_claim_database",
        "_claim_shard_manifests",
        "_export_claim_index",
        "_build_claim_index_files",
        "_claim_shard_manifests_from_paths",
        "_write_claim_index_manifest",
        "_assert_lake_wide_claims",
        "_publish_or_validate_claim_index",
        "load_normalized_snapshot_v3",
        "_arrow_ready_rows",
        "_logical_rows",
    )
    targets = [(normalized_v3, name, name) for name in function_names] + [
        (normalized_v3._InputSpool, "flush", "_InputSpool.flush"),
        (normalized_v3._OpenPartition, "flush", "_OpenPartition.flush"),
        (normalized_v3._JsonArrayDigest, "update", "_JsonArrayDigest.update"),
    ]
    originals = {
        label: (owner, attribute, getattr(owner, attribute)) for owner, attribute, label in targets
    }
    timings: dict[str, dict[str, float | int]] = {
        label: {"calls": 0, "seconds": 0.0} for label in originals
    }

    for label, (owner, attribute, original) in originals.items():

        @wraps(original)
        def measured(
            *args: Any, _label: str = label, _original: Any = original, **kwargs: Any
        ) -> Any:
            started = time.perf_counter()
            try:
                return _original(*args, **kwargs)
            finally:
                timing = timings[_label]
                timing["calls"] = int(timing["calls"]) + 1
                timing["seconds"] = float(timing["seconds"]) + time.perf_counter() - started

        setattr(owner, attribute, measured)
    try:
        yield timings
    finally:
        for owner, attribute, original in originals.values():
            setattr(owner, attribute, original)


def _git_identity(repository: Path) -> dict[str, Any]:
    def git(*arguments: str) -> str:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    return {
        "commit": git("rev-parse", "HEAD"),
        "branch": git("branch", "--show-current"),
        "dirty": bool(git("status", "--porcelain")),
    }


def _tree_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _disk_roots(work_root: Path, repository: Path) -> tuple[Path, ...]:
    candidates = [
        Path(work_root.anchor),
        Path(repository.anchor),
        Path(tempfile.gettempdir()).resolve(),
        *(Path(f"{letter}:\\") for letter in ("C", "H", "F")),
    ]
    roots: dict[str, Path] = {}
    for candidate in candidates:
        if not candidate.exists():
            continue
        root = Path(candidate.anchor or candidate)
        roots[str(root).casefold()] = root
    return tuple(roots[key] for key in sorted(roots))


def _disk_usage(roots: tuple[Path, ...]) -> dict[str, dict[str, int]]:
    return {
        str(root): {
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
        }
        for root in roots
        for usage in (shutil.disk_usage(root),)
    }


def _run_once_independent(
    work_root: Path,
    rows: int,
    run_number: int,
    batch_rows: int,
    policy: StoragePolicy,
) -> dict[str, Any]:
    worker_output = work_root / f".worker-result-{run_number:02d}.json"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--work-root",
        str(work_root),
        "--output",
        str(worker_output),
        "--rows",
        str(rows),
        "--batch-rows",
        str(batch_rows),
        "--worker-run",
        str(run_number),
        "--hot-quota-bytes",
        str(policy.hot_quota_bytes),
        "--minimum-free-bytes",
        str(policy.minimum_free_bytes),
        "--minimum-free-fraction",
        str(policy.minimum_free_fraction),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            "Independent benchmark worker failed: "
            f"run={run_number}, stdout={completed.stdout!r}, stderr={completed.stderr!r}"
        )
    result = json.loads(worker_output.read_text(encoding="utf-8"))
    worker_output.unlink()
    return result


def _run_once(
    work_root: Path,
    rows: int,
    run_number: int,
    batch_rows: int,
    policy: StoragePolicy,
) -> dict[str, Any]:
    run_root = work_root / f"run-{run_number:02d}"
    run_root.mkdir(parents=True, exist_ok=False)
    raw = write_raw_bytes(
        run_root,
        source=_PROVIDER,
        request={"fixture": "m7-synthetic-l2-v1", "rows": rows},
        collected_at=_TIMESTAMP,
        payload=b"m7-synthetic-l2-v1",
        idempotency_key=f"m7-synthetic-l2-v1-{rows}",
        policy=policy,
    )
    started = time.perf_counter()
    with _profile_storage_stages() as stage_timings, _PeakRssSampler() as memory:
        result = write_normalized_batches(
            run_root,
            synthetic_l2_batches(rows, batch_rows),
            provider=_PROVIDER,
            venue=_VENUE,
            upstream_raw_references=[raw.reference()],
            policy=policy,
        )
        if result.snapshot is None:
            raise RuntimeError("M7 benchmark produced no normalized snapshot")
        snapshot = result.snapshot
        strict_snapshot = load_normalized_snapshot(run_root, snapshot.snapshot_id)
        strict_identity = (
            strict_snapshot.snapshot_id,
            strict_snapshot.logical_sha256,
            strict_snapshot.rows,
            strict_snapshot.partitions,
            strict_snapshot.event_claim_index,
            strict_snapshot.l2_checkpoints,
        )
        expected_identity = (
            snapshot.snapshot_id,
            snapshot.logical_sha256,
            snapshot.rows,
            snapshot.partitions,
            snapshot.event_claim_index,
            snapshot.l2_checkpoints,
        )
        if strict_identity != expected_identity:
            raise RuntimeError("Strict normalized snapshot reload changed published identity")
    elapsed = time.perf_counter() - started
    final_checkpoint = snapshot.l2_checkpoints[0]
    manifest_path = run_root / "normalized" / "snapshots" / snapshot.snapshot_id / "manifest.json"
    return {
        "run": run_number,
        "rows": rows,
        "accepted_rows": result.accepted_rows,
        "quarantined_rows": result.quarantined_rows,
        "elapsed_seconds": elapsed,
        "events_per_second": rows / elapsed,
        "peak_rss_bytes": memory.peak_bytes,
        "peak_rss_gib": memory.peak_bytes / _GIB,
        "stage_timings": stage_timings,
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_logical_sha256": snapshot.logical_sha256,
        "snapshot_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "strict_reload_verified": True,
        "partition_manifests": [asdict(item) for item in snapshot.partitions],
        "claim_index": asdict(snapshot.event_claim_index),
        "final_l2_checkpoint": asdict(final_checkpoint),
        "retained_bytes": _tree_bytes(run_root),
    }


def run_benchmark(
    work_root: Path,
    *,
    rows: int,
    runs: int,
    minimum_events_per_second: float,
    maximum_peak_rss_gib: float,
    batch_rows: int,
    repository: Path,
    policy: StoragePolicy | None = None,
    independent_processes: bool = False,
    cleanup_work_data: bool = False,
) -> dict[str, Any]:
    if rows < 2:
        raise ValueError("rows must be at least 2")
    if runs < 1:
        raise ValueError("runs must be positive")
    effective_policy = policy or StoragePolicy()
    roots = _disk_roots(work_root, repository)
    disk_before = _disk_usage(roots)
    work_root.mkdir(parents=True, exist_ok=False)
    runner = _run_once_independent if independent_processes else _run_once
    run_results = [
        runner(work_root, rows, run_number, batch_rows, effective_policy)
        for run_number in range(1, runs + 1)
    ]
    deterministic_fields = (
        "snapshot_id",
        "snapshot_logical_sha256",
        "snapshot_manifest_sha256",
        "partition_manifests",
        "claim_index",
        "final_l2_checkpoint",
        "strict_reload_verified",
    )
    deterministic = all(
        all(result[field] == run_results[0][field] for field in deterministic_fields)
        for result in run_results[1:]
    )
    minimum_throughput = min(item["events_per_second"] for item in run_results)
    maximum_rss = max(item["peak_rss_gib"] for item in run_results)
    accepted = all(
        item["accepted_rows"] == rows and item["quarantined_rows"] == 0 for item in run_results
    )
    gates = {
        "all_rows_accepted": accepted,
        "deterministic_artifacts": deterministic,
        "minimum_throughput": minimum_throughput >= minimum_events_per_second,
        "maximum_peak_rss": maximum_rss <= maximum_peak_rss_gib,
    }
    generated_bytes = sum(item["retained_bytes"] for item in run_results)
    cleanup_performed = False
    if cleanup_work_data:
        resolved_work_root = work_root.resolve()
        if resolved_work_root == Path(resolved_work_root.anchor):
            raise RuntimeError("Refusing to clean a volume root")
        shutil.rmtree(resolved_work_root)
        cleanup_performed = True
    report = {
        "schema_version": "1.0.0",
        "benchmark": "quant-data-kit-normalized-v3-synthetic-l2",
        "measurement_scope": (
            "write_normalized_batches end-to-end from generated schema-exact RecordBatch: "
            "vector validation, PIT/sequence/L2 reconstruction, Parquet, claims, immutable "
            "publish, and verified reload; Raw admission setup excluded"
        ),
        "input_contract": "Iterable[pyarrow.RecordBatch]",
        "fixture": "one valid snapshot followed by same-book sequential upsert deltas",
        "rows_per_run": rows,
        "record_batch_rows": batch_rows,
        "runs": runs,
        "thresholds": {
            "minimum_events_per_second": minimum_events_per_second,
            "maximum_peak_rss_gib": maximum_peak_rss_gib,
            "storage_policy": asdict(effective_policy),
        },
        "summary": {
            "minimum_events_per_second": minimum_throughput,
            "median_events_per_second": statistics.median(
                item["events_per_second"] for item in run_results
            ),
            "maximum_peak_rss_gib": maximum_rss,
            "retained_bytes": sum(item["retained_bytes"] for item in run_results),
        },
        "gates": gates,
        "passed": all(gates.values()),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "logical_cpu_count": os.cpu_count(),
            "quant_data_kit": version("quant-data-kit"),
            "duckdb": version("duckdb"),
            "jsonschema_rs": version("jsonschema-rs"),
            "orjson": version("orjson"),
            "pyarrow": version("pyarrow"),
            "psutil": version("psutil"),
            "git": _git_identity(repository),
            "temp_environment": {
                "TEMP": os.environ.get("TEMP"),
                "TMP": os.environ.get("TMP"),
                "tempfile_gettempdir": tempfile.gettempdir(),
            },
        },
        "execution": {
            "independent_processes": independent_processes,
            "work_root": str(work_root.resolve()),
            "disk_usage_before": disk_before,
            "disk_usage_after": _disk_usage(roots),
            "data_retention": {
                "generated_bytes": generated_bytes,
                "cleanup_requested": cleanup_work_data,
                "cleanup_performed": cleanup_performed,
                "work_root_exists_after": work_root.exists(),
            },
        },
        "run_results": run_results,
    }
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--batch-rows", type=int, default=262_144)
    parser.add_argument("--minimum-events-per-second", type=float, default=100_000.0)
    parser.add_argument("--maximum-peak-rss-gib", type=float, default=16.0)
    parser.add_argument("--cleanup-work-data", action="store_true")
    parser.add_argument("--worker-run", type=int)
    parser.add_argument("--hot-quota-bytes", type=int, default=StoragePolicy().hot_quota_bytes)
    parser.add_argument(
        "--minimum-free-bytes", type=int, default=StoragePolicy().minimum_free_bytes
    )
    parser.add_argument(
        "--minimum-free-fraction",
        type=float,
        default=StoragePolicy().minimum_free_fraction,
    )
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    repository = Path(__file__).resolve().parents[1]
    policy = StoragePolicy(
        hot_quota_bytes=arguments.hot_quota_bytes,
        minimum_free_bytes=arguments.minimum_free_bytes,
        minimum_free_fraction=arguments.minimum_free_fraction,
    )
    if arguments.worker_run is not None:
        result = _run_once(
            arguments.work_root.resolve(),
            arguments.rows,
            arguments.worker_run,
            arguments.batch_rows,
            policy,
        )
        output = arguments.output.resolve()
        output.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
        return 0
    report = run_benchmark(
        arguments.work_root.resolve(),
        rows=arguments.rows,
        runs=arguments.runs,
        minimum_events_per_second=arguments.minimum_events_per_second,
        maximum_peak_rss_gib=arguments.maximum_peak_rss_gib,
        batch_rows=arguments.batch_rows,
        repository=repository,
        policy=policy,
        independent_processes=True,
        cleanup_work_data=arguments.cleanup_work_data,
    )
    output = arguments.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    print(f"passed={report['passed']} report={output}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
