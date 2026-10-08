from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tools import check_branch_coverage


def _git_repository(tmp_path: Path) -> tuple[Path, tuple[str, ...]]:
    repository = tmp_path / "repository"
    sources = (
        "src/quant_data_kit/core.py",
        "src/quant_data_kit/extra.py",
        "src/quant_data_kit/sub/nested.py",
    )
    for name in sources:
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "--quiet", str(repository)], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "add", "--", "src/quant_data_kit"],
        check=True,
    )
    return repository, sources


def _details(covered: int, total: int) -> dict:
    return {"summary": {"covered_branches": covered, "num_branches": total}}


def _complete_report(repository: Path) -> dict:
    return {
        "files": {
            (repository / "src/quant_data_kit/core.py").as_posix(): _details(9, 10),
            r"C:\checkout\src\quant_data_kit\extra.py": _details(1, 1),
            "src/quant_data_kit/sub/nested.py": _details(0, 0),
            "tests/test_core.py": _details(100, 100),
            "tools/helper.py": _details(100, 100),
        }
    }


def _evaluate(report: dict, repository: Path) -> tuple[bool, list[str]]:
    return check_branch_coverage._evaluate_report(
        report,
        repository,
        core_thresholds={"src/quant_data_kit/core.py": 90},
        all_source_threshold=80,
    )


def test_complete_git_source_set_accepts_windows_and_unix_paths(tmp_path: Path) -> None:
    repository, sources = _git_repository(tmp_path)
    assert check_branch_coverage._tracked_source_files(repository) == set(sources)
    passed, messages = _evaluate(_complete_report(repository), repository)
    assert passed is True
    assert "TRACKED SOURCE FILES: 3" in messages
    assert any(message.startswith("ALL tracked src/quant_data_kit: 10/11=") for message in messages)


def test_missing_noncore_and_core_sources_fail(tmp_path: Path) -> None:
    repository, _ = _git_repository(tmp_path)
    noncore_missing = _complete_report(repository)
    noncore_missing["files"].pop(r"C:\checkout\src\quant_data_kit\extra.py")
    passed, messages = _evaluate(noncore_missing, repository)
    assert passed is False
    assert "MISSING SOURCE: src/quant_data_kit/extra.py" in messages

    core_missing = _complete_report(repository)
    core_missing["files"].pop((repository / "src/quant_data_kit/core.py").as_posix())
    passed, messages = _evaluate(core_missing, repository)
    assert passed is False
    assert "MISSING SOURCE: src/quant_data_kit/core.py" in messages
    assert "MISSING CORE COVERAGE: src/quant_data_kit/core.py" in messages


def test_unknown_source_and_normalized_duplicate_fail(tmp_path: Path) -> None:
    repository, _ = _git_repository(tmp_path)
    unknown = _complete_report(repository)
    unknown["files"]["src/quant_data_kit/unknown.py"] = _details(100, 100)
    passed, messages = _evaluate(unknown, repository)
    assert passed is False
    assert "UNKNOWN SOURCE: src/quant_data_kit/unknown.py" in messages

    duplicate = _complete_report(repository)
    duplicate["files"][r"D:\other\src\quant_data_kit\core.py"] = _details(9, 10)
    with pytest.raises(check_branch_coverage.CoverageGateError, match="collide"):
        _evaluate(duplicate, repository)


@pytest.mark.parametrize(
    "summary",
    [
        {"covered_branches": 2, "num_branches": 1},
        {"covered_branches": -1, "num_branches": 1},
        {"covered_branches": True, "num_branches": 1},
        {"covered_branches": 1},
    ],
)
def test_invalid_branch_counts_fail(tmp_path: Path, summary: dict) -> None:
    repository, _ = _git_repository(tmp_path)
    report = _complete_report(repository)
    report["files"][r"C:\checkout\src\quant_data_kit\extra.py"] = {"summary": summary}
    passed, messages = _evaluate(report, repository)
    assert passed is False
    assert any(message.startswith("INVALID:") for message in messages)


def test_cli_uses_git_inventory_and_fails_without_git(tmp_path: Path) -> None:
    repository, _ = _git_repository(tmp_path)
    report_path = tmp_path / "coverage.json"
    report_path.write_text(json.dumps(_complete_report(repository)), encoding="utf-8")
    original_thresholds = check_branch_coverage.CORE_THRESHOLDS
    original_all_threshold = check_branch_coverage.ALL_SOURCE_THRESHOLD
    try:
        check_branch_coverage.CORE_THRESHOLDS = {"src/quant_data_kit/core.py": 90}
        check_branch_coverage.ALL_SOURCE_THRESHOLD = 80
        assert check_branch_coverage.main([str(report_path)], repository=repository) == 0
    finally:
        check_branch_coverage.CORE_THRESHOLDS = original_thresholds
        check_branch_coverage.ALL_SOURCE_THRESHOLD = original_all_threshold

    with pytest.raises(check_branch_coverage.CoverageGateError, match="Git"):
        check_branch_coverage._tracked_source_files(tmp_path / "not-a-repository")


def test_noncanonical_parent_path_fails() -> None:
    with pytest.raises(check_branch_coverage.CoverageGateError, match="not canonical"):
        check_branch_coverage._normalized_files(
            {"files": {"src/quant_data_kit/../escape.py": _details(1, 1)}}
        )
