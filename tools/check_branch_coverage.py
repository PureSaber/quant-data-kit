"""Enforce pure branch-coverage ratios from coverage.py JSON output."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

CORE_THRESHOLDS = {
    "src/quant_data_kit/data_lake.py": 90,
    "src/quant_data_kit/curated.py": 90,
    "src/quant_data_kit/process_lock.py": 90,
    "src/quant_data_kit/schemas_v2.py": 90,
    "src/quant_data_kit/l2_replay.py": 90,
    "src/quant_data_kit/adapters_v2/base.py": 90,
    "src/quant_data_kit/adapters_v2/binance.py": 90,
    "src/quant_data_kit/adapters_v2/okx.py": 90,
    "src/quant_data_kit/adapters_v2/cn_neutral.py": 90,
}
ALL_SOURCE_THRESHOLD = 80


def _normalized_files(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {name.replace("\\", "/"): details for name, details in report["files"].items()}


def _branch_counts(details: dict[str, Any]) -> tuple[int, int]:
    summary = details["summary"]
    return int(summary["covered_branches"]), int(summary["num_branches"])


def _format_result(label: str, covered: int, total: int, required: int) -> str:
    ratio = 100 * covered / total if total else 100.0
    return f"{label}: {covered}/{total}={ratio:.2f}% (required>={required}%)"


def main() -> int:
    report_path = Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.json")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    files = _normalized_files(report)
    failed = False
    for filename, required in CORE_THRESHOLDS.items():
        if filename not in files:
            print(f"MISSING: {filename}")
            failed = True
            continue
        covered, total = _branch_counts(files[filename])
        print(_format_result(filename, covered, total, required))
        if total == 0 or covered * 100 < required * total:
            failed = True

    source_counts = [
        _branch_counts(details)
        for filename, details in files.items()
        if filename.startswith("src/quant_data_kit/")
    ]
    covered = sum(item[0] for item in source_counts)
    total = sum(item[1] for item in source_counts)
    print(_format_result("ALL src/quant_data_kit", covered, total, ALL_SOURCE_THRESHOLD))
    if total == 0 or covered * 100 < ALL_SOURCE_THRESHOLD * total:
        failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
