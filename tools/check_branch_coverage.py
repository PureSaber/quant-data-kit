"""Enforce complete, pure branch-coverage gates for tracked package sources."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

CORE_THRESHOLDS = {
    "src/quant_data_kit/financial/common.py": 90,
    "src/quant_data_kit/financial/publication_time_v2.py": 90,
    "src/quant_data_kit/financial/dividends_v2.py": 90,
    "src/quant_data_kit/financial/dividend_migration_v2.py": 90,
    "src/quant_data_kit/normalized_v3.py": 90,
    "src/quant_data_kit/data_lake.py": 90,
    "src/quant_data_kit/curated.py": 90,
    "src/quant_data_kit/research_contracts_v2.py": 90,
    "src/quant_data_kit/research_inputs_v2.py": 90,
    "src/quant_data_kit/process_lock.py": 90,
    "src/quant_data_kit/schemas_v2.py": 90,
    "src/quant_data_kit/temporal_v2.py": 90,
    "src/quant_data_kit/l2_replay.py": 90,
    "src/quant_data_kit/adapters_v2/base.py": 90,
    "src/quant_data_kit/adapters_v2/binance.py": 90,
    "src/quant_data_kit/adapters_v2/okx.py": 90,
    "src/quant_data_kit/adapters_v2/cn_neutral.py": 90,
    "src/quant_data_kit/capture_v2/models.py": 90,
    "src/quant_data_kit/capture_v2/storage.py": 90,
    "src/quant_data_kit/capture_v2/synchronizers.py": 90,
    "src/quant_data_kit/capture_v2/epoch.py": 90,
    "src/quant_data_kit/capture_v2/transport.py": 90,
    "src/quant_data_kit/capture_v2/collector.py": 90,
    "src/quant_data_kit/capture_v2/cli.py": 90,
}
ALL_SOURCE_THRESHOLD = 80
_SOURCE_PREFIX = "src/quant_data_kit/"


class CoverageGateError(ValueError):
    """Raised when coverage evidence cannot be validated safely."""


def _normalized_path(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise CoverageGateError("coverage report contains an invalid file path")
    normalized = value.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    marker = "/" + _SOURCE_PREFIX
    prefixed = "/" + normalized.lstrip("/")
    if marker in prefixed:
        normalized = _SOURCE_PREFIX + prefixed.split(marker, 1)[1]
    elif normalized.startswith("quant_data_kit/"):
        normalized = "src/" + normalized
    if ".." in PurePosixPath(normalized).parts:
        raise CoverageGateError(f"coverage report path is not canonical: {value}")
    return normalized


def _normalized_files(report: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    raw_files = report.get("files")
    if not isinstance(raw_files, Mapping):
        raise CoverageGateError("coverage report has no files mapping")
    files: dict[str, Mapping[str, Any]] = {}
    origins: dict[str, str] = {}
    for raw_name, details in raw_files.items():
        name = _normalized_path(raw_name)
        if name in files:
            raise CoverageGateError(
                "coverage report paths collide after normalization: "
                f"{origins[name]!r}, {raw_name!r} -> {name}"
            )
        if not isinstance(details, Mapping):
            raise CoverageGateError(f"coverage details are invalid: {raw_name}")
        files[name] = details
        origins[name] = raw_name
    return files


def _tracked_source_files(repository: Path) -> set[str]:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository), "ls-files", "-z", "--", "src/quant_data_kit"],
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise CoverageGateError(f"cannot read tracked source files with Git: {exc}") from exc
    if completed.returncode != 0:
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise CoverageGateError(f"cannot read tracked source files with Git: {message}")
    try:
        names = [item.decode("utf-8") for item in completed.stdout.split(b"\0") if item]
    except UnicodeDecodeError as exc:
        raise CoverageGateError("Git returned a non-UTF-8 source path") from exc
    tracked = {
        _normalized_path(name)
        for name in names
        if name.endswith(".py") and name.replace("\\", "/").startswith(_SOURCE_PREFIX)
    }
    if not tracked:
        raise CoverageGateError("Git returned no tracked Python package sources")
    return tracked


def _branch_counts(details: Mapping[str, Any], filename: str) -> tuple[int, int]:
    summary = details.get("summary")
    if not isinstance(summary, Mapping):
        raise CoverageGateError(f"coverage summary is missing: {filename}")
    covered = summary.get("covered_branches")
    total = summary.get("num_branches")
    if type(covered) is not int or type(total) is not int:
        raise CoverageGateError(f"coverage branch counts are not integers: {filename}")
    if covered < 0 or total < 0 or covered > total:
        raise CoverageGateError(
            f"coverage branch counts are invalid: {filename}: covered={covered}, total={total}"
        )
    return covered, total


def _format_result(label: str, covered: int, total: int, required: int) -> str:
    ratio = 100 * covered / total if total else 100.0
    return f"{label}: {covered}/{total}={ratio:.2f}% (required>={required}%)"


def _evaluate_report(
    report: Mapping[str, Any],
    repository: Path,
    *,
    core_thresholds: Mapping[str, int] | None = None,
    all_source_threshold: int | None = None,
) -> tuple[bool, list[str]]:
    core_thresholds = CORE_THRESHOLDS if core_thresholds is None else core_thresholds
    all_source_threshold = (
        ALL_SOURCE_THRESHOLD if all_source_threshold is None else all_source_threshold
    )
    tracked = _tracked_source_files(repository)
    files = _normalized_files(report)
    reported_sources = {name for name in files if name.startswith(_SOURCE_PREFIX)}
    missing = sorted(tracked - reported_sources)
    unknown = sorted(reported_sources - tracked)
    messages = [f"TRACKED SOURCE FILES: {len(tracked)}"]
    messages.extend(f"MISSING SOURCE: {name}" for name in missing)
    messages.extend(f"UNKNOWN SOURCE: {name}" for name in unknown)
    failed = bool(missing or unknown)

    untracked_core = sorted(set(core_thresholds) - tracked)
    messages.extend(f"UNTRACKED CORE GATE: {name}" for name in untracked_core)
    failed = failed or bool(untracked_core)

    counts: dict[str, tuple[int, int]] = {}
    for filename in sorted(tracked & reported_sources):
        try:
            counts[filename] = _branch_counts(files[filename], filename)
        except CoverageGateError as exc:
            messages.append(f"INVALID: {exc}")
            failed = True

    for filename, required in core_thresholds.items():
        if filename not in counts:
            messages.append(f"MISSING CORE COVERAGE: {filename}")
            failed = True
            continue
        covered, total = counts[filename]
        messages.append(_format_result(filename, covered, total, required))
        if total == 0 or covered * 100 < required * total:
            failed = True

    covered = sum(item[0] for item in counts.values())
    total = sum(item[1] for item in counts.values())
    messages.append(
        _format_result("ALL tracked src/quant_data_kit", covered, total, all_source_threshold)
    )
    if len(counts) != len(tracked) or total == 0 or covered * 100 < all_source_threshold * total:
        failed = True
    return not failed, messages


def main(
    argv: Sequence[str] | None = None,
    *,
    repository: Path | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    report_path = Path(arguments[0] if arguments else "coverage.json")
    repository = repository or Path(__file__).resolve().parents[1]
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if not isinstance(report, Mapping):
            raise CoverageGateError("coverage report root must be an object")
        passed, messages = _evaluate_report(report, repository)
    except (OSError, UnicodeError, json.JSONDecodeError, CoverageGateError) as exc:
        print(f"ERROR: {exc}")
        return 1
    for message in messages:
        print(message)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
