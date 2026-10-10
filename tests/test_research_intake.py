from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from quant_data_kit import research_intake as intake


def _bars_contract(**overrides):
    contract = {
        "schema_version": intake.CONTRACT_SCHEMA_VERSION,
        "kind": "daily_bars",
        "input": {"format": "csv", "encoding": "utf-8", "delimiter": ","},
        "mapping": {
            "symbol": "ticker",
            "date": "trading_day",
            "open": "open_px",
            "high": "high_px",
            "low": "low_px",
            "close": "close_px",
            "volume": "volume_shares",
        },
        "types": {
            "symbol": "string",
            "date": "date",
            "open": "number",
            "high": "number",
            "low": "number",
            "close": "number",
            "volume": "integer",
        },
        "formats": {"date": "%Y-%m-%d"},
        "primary_key": ["symbol", "date"],
        "metadata": {
            "source": "licensed-desk-export",
            "provider": "test-vendor",
            "units": {
                "open": "CNY",
                "high": "CNY",
                "low": "CNY",
                "close": "CNY",
                "volume": "share",
            },
            "timezone": "Asia/Shanghai",
            "adjustment": "raw",
        },
        "limits": {"max_bytes": 1_000_000, "max_rows": 1_000},
    }
    for key, value in overrides.items():
        contract[key] = value
    return contract


def _write_csv(path: Path, rows: list[dict[str, object]]) -> bytes:
    body = pd.DataFrame(rows).to_csv(index=False).encode("utf-8")
    path.write_bytes(body)
    return body


def _rows():
    return [
        {
            "ticker": "000001",
            "trading_day": "2026-01-02",
            "open_px": "10.00",
            "high_px": "11.00",
            "low_px": "9.50",
            "close_px": "10.50",
            "volume_shares": "100",
        },
        {
            "ticker": "000002",
            "trading_day": "2026-01-05",
            "open_px": "20.00",
            "high_px": "21.00",
            "low_px": "19.50",
            "close_px": "20.50",
            "volume_shares": "200",
        },
    ]


def test_inspect_source_is_bounded_and_preserves_strings(tmp_path):
    source = tmp_path / "bars.csv"
    _write_csv(source, _rows() * 15)

    result = intake.inspect_source(
        source,
        file_format="csv",
        encoding="utf-8",
        delimiter=",",
        max_sample_rows=20,
    )

    assert result["sample_only"] is True
    assert result["quality_checked"] is False
    assert result["sample_rows"] == 20
    assert result["rows"][0]["ticker"] == "000001"
    assert result["columns"][0] == {"name": "ticker", "type": "string"}
    assert result["structure_validation"] == {
        "rows_checked": 20,
        "complete": False,
    }


def test_duplicate_raw_header_fails_before_pandas_can_mangle_it(tmp_path):
    source = tmp_path / "duplicate.csv"
    raw = (
        b"ticker,trading_day,open_px,high_px,low_px,close_px,close_px,volume_shares\n"
        b"000001,2026-01-02,10,11,9.5,10.5,99,100\n"
    )
    source.write_bytes(raw)

    with pytest.raises(intake.ResearchIntakeError, match="duplicate raw header"):
        intake.inspect_source(source, encoding="utf-8", delimiter=",")

    root = tmp_path / "catalog"
    failed = intake.import_dataset(root, source, "bars", _bars_contract())
    assert failed["receipt"]["status"] == "failed"
    assert failed["version"] is None
    assert (root / failed["receipt"]["raw_path"]).read_bytes() == raw


def test_delimited_width_is_full_scanned_while_inspection_marks_sample_scope(tmp_path):
    source = tmp_path / "wide.csv"
    valid = [f"{index:06d},{index}" for index in range(20)]
    raw = ("code,value\n" + "\n".join(valid + ["000020,20,200"]) + "\n").encode()
    source.write_bytes(raw)

    inspected = intake.inspect_source(
        source,
        encoding="utf-8",
        delimiter=",",
        max_sample_rows=20,
    )
    assert inspected["quality_checked"] is False
    assert inspected["structure_validation"] == {
        "rows_checked": 20,
        "complete": False,
    }

    contract = {
        "schema_version": intake.CONTRACT_SCHEMA_VERSION,
        "kind": "table",
        "input": {"format": "csv", "encoding": "utf-8", "delimiter": ","},
        "mapping": {"code": "code", "value": "value"},
        "types": {"code": "string", "value": "number"},
        "formats": {},
        "primary_key": ["code"],
        "metadata": {"source": "test-export", "units": {}},
    }
    root = tmp_path / "catalog"
    failed = intake.import_dataset(root, source, "wide", contract)
    assert failed["receipt"]["status"] == "failed"
    assert "has 3 fields; header has 2" in failed["error"]["message"]
    assert (root / failed["receipt"]["raw_path"]).read_bytes() == raw


def test_import_read_preserves_leading_zero_and_raw_bytes(tmp_path):
    source = tmp_path / "bars.csv"
    original = _write_csv(source, _rows())
    root = tmp_path / "catalog"

    imported = intake.import_dataset(root, source, "bars", _bars_contract())

    assert imported["receipt"]["status"] == "succeeded"
    assert imported["version"]["status"] == "ready"
    loaded = intake.read_dataset(root, "bars", purpose="daily_bars_research")
    assert loaded.frame["symbol"].tolist() == ["000001", "000002"]
    assert loaded.check["allowed"] is True
    assert loaded.metadata["market_certified"] is False
    raw_path = root / "versions" / imported["version"]["id"] / imported["version"]["raw"]["path"]
    assert raw_path.read_bytes() == original
    assert source.read_bytes() == original
    assert hashlib.sha256(original).hexdigest() == imported["version"]["raw_sha256"]


def test_ambiguous_date_and_capacity_failure_keep_raw_and_do_not_advance_latest(tmp_path):
    root = tmp_path / "catalog"
    good_source = tmp_path / "good.csv"
    _write_csv(good_source, _rows())
    parent = intake.import_dataset(root, good_source, "bars", _bars_contract())["version"]

    ambiguous_source = tmp_path / "ambiguous.csv"
    ambiguous_rows = _rows()
    ambiguous_rows[0]["trading_day"] = "01/02/2026"
    ambiguous_body = _write_csv(ambiguous_source, ambiguous_rows)
    contract = _bars_contract()
    contract["formats"] = {}
    blocked = intake.import_dataset(root, ambiguous_source, "bars", contract)

    assert blocked["version"]["status"] == "blocked"
    assert intake.list_datasets(root)["datasets"][0]["latest"] == parent["id"]
    raw_path = root / "versions" / blocked["version"]["id"] / blocked["version"]["raw"]["path"]
    assert raw_path.read_bytes() == ambiguous_body
    assert any(issue["rule"] == "explicit_date_format" for issue in blocked["report"]["issues"])

    too_large = _bars_contract(limits={"max_bytes": 10, "max_rows": 1_000})
    failed = intake.import_dataset(root, ambiguous_source, "bars", too_large)
    assert failed["version"] is None
    assert failed["receipt"]["status"] == "failed"
    shown = intake.show_dataset(root, receipt=failed["receipt"]["id"])
    assert shown["status"] == "failed"
    assert shown["raw_sha256"] == hashlib.sha256(ambiguous_body).hexdigest()
    assert intake.list_datasets(root)["datasets"][0]["latest"] == parent["id"]


def test_invalid_contract_and_stale_parent_retain_raw_without_replacing_latest(tmp_path):
    root = tmp_path / "catalog"
    first_source = tmp_path / "first.csv"
    _write_csv(first_source, _rows())
    first = intake.import_dataset(root, first_source, "bars", _bars_contract())["version"]

    second_source = tmp_path / "second.csv"
    second_rows = _rows()
    second_rows[0]["close_px"] = "10.75"
    second_body = _write_csv(second_source, second_rows)
    second = intake.import_dataset(root, second_source, "bars", _bars_contract())["version"]

    stale = intake.import_dataset(
        root,
        second_source,
        "bars",
        _bars_contract(),
        parent=first["id"],
    )
    assert stale["receipt"]["status"] == "failed"
    assert stale["receipt"]["raw_sha256"] == hashlib.sha256(second_body).hexdigest()
    assert intake.list_datasets(root)["datasets"][0]["latest"] == second["id"]

    invalid_contract = _bars_contract()
    invalid_contract["mapping"].pop("close")
    invalid_contract["types"].pop("close")
    invalid = intake.import_dataset(root, second_source, "bars", invalid_contract)
    assert invalid["receipt"]["status"] == "failed"
    assert invalid["receipt"]["raw_sha256"] == hashlib.sha256(second_body).hexdigest()
    assert (root / invalid["receipt"]["raw_path"]).read_bytes() == second_body
    assert intake.list_datasets(root)["datasets"][0]["latest"] == second["id"]


def test_empty_research_refresh_keeps_latest_and_empty_strict_scope_is_rejected(tmp_path):
    root = tmp_path / "catalog"
    source = tmp_path / "bars.csv"
    _write_csv(source, _rows())
    parent = intake.import_dataset(root, source, "bars", _bars_contract())["version"]

    empty_source = tmp_path / "empty.csv"
    pd.DataFrame(_rows()).head(0).to_csv(empty_source, index=False)
    empty = intake.import_dataset(
        root,
        empty_source,
        "bars",
        _bars_contract(),
        parent=parent["id"],
    )

    assert empty["version"]["status"] == "blocked"
    assert any(issue["rule"] == "empty_research_dataset" for issue in empty["report"]["issues"])
    assert intake.list_datasets(root)["datasets"][0]["latest"] == parent["id"]
    outside = intake.check_dataset(
        root,
        "bars",
        version=parent["id"],
        purpose="daily_bars_research",
        start="2030-01-01",
        end="2030-01-31",
    )
    assert outside["allowed"] is False
    assert any(issue["rule"] == "empty_research_scope" for issue in outside["issues"])

    table_source = tmp_path / "empty-table.csv"
    table_source.write_text("code,value\n", encoding="utf-8")
    table_contract = {
        "schema_version": intake.CONTRACT_SCHEMA_VERSION,
        "kind": "table",
        "input": {"format": "csv", "encoding": "utf-8", "delimiter": ","},
        "mapping": {"code": "code", "value": "value"},
        "types": {"code": "string", "value": "number"},
        "formats": {},
        "primary_key": [],
        "metadata": {"source": "empty-export", "units": {}},
    }
    table = intake.import_dataset(root, table_source, "empty-table", table_contract)
    assert table["version"]["status"] == "ready"
    assert table["report"]["issues"][0]["rule"] == "empty_table"
    assert table["report"]["issues"][0]["severity"] == "warning"


def test_unknown_unit_blocks_only_strict_usage_of_that_column(tmp_path):
    source = tmp_path / "bars.csv"
    _write_csv(source, _rows())
    contract = _bars_contract()
    contract["metadata"]["units"].pop("volume")
    root = tmp_path / "catalog"
    version = intake.import_dataset(root, source, "bars", contract)["version"]

    close_only = intake.check_dataset(
        root,
        "bars",
        version=version["id"],
        purpose="daily_bars_research",
        columns=["symbol", "date", "close"],
    )
    with_volume = intake.check_dataset(
        root,
        "bars",
        version=version["id"],
        purpose="daily_bars_research",
        columns=["symbol", "date", "close", "volume"],
    )

    assert close_only["allowed"] is True
    assert with_volume["allowed"] is False
    assert any(issue["rule"] == "declared_units" for issue in with_volume["issues"])


def test_conflicts_invalid_values_scope_and_full_scan(tmp_path):
    source = tmp_path / "bars.csv"
    rows = _rows()
    rows.extend(
        [
            {
                **rows[1],
                "close_px": "bad",
            },
            {
                **rows[1],
                "close_px": "21.50",
            },
        ]
    )
    _write_csv(source, rows)
    root = tmp_path / "catalog"
    imported = intake.import_dataset(root, source, "bars", _bars_contract())

    assert imported["version"]["status"] == "blocked"
    issues = {issue["rule"]: issue for issue in imported["report"]["issues"]}
    assert issues["type_number"]["affected_count"] == 1
    assert issues["duplicate_primary_key_conflict"]["affected_count"] == 3
    assert len(issues["duplicate_primary_key_conflict"]["samples"]) <= intake.MAX_ISSUE_SAMPLES
    normalized_path = root / "versions" / imported["version"]["id"] / "normalized.parquet"
    normalized = pd.read_parquet(normalized_path)
    assert len(normalized) == 4
    assert normalized["close"].isna().sum() == 1

    scoped = intake.check_dataset(
        root,
        "bars",
        version=imported["version"]["id"],
        purpose="daily_bars_research",
        symbols=["000001"],
        start="2026-01-02",
        end="2026-01-02",
    )
    full = intake.check_dataset(
        root,
        "bars",
        version=imported["version"]["id"],
        purpose="daily_bars_research",
    )
    assert scoped["allowed"] is True
    assert scoped["scope"]["matched_rows"] == 1
    assert scoped["scope"]["full_scan"] is False
    assert full["allowed"] is False
    assert full["scope"]["full_scan"] is True


def test_unknown_date_cannot_be_hidden_by_scope_and_boundaries_are_strict(tmp_path):
    source = tmp_path / "bars.csv"
    rows = _rows()
    rows[1]["trading_day"] = "not-a-date"
    _write_csv(source, rows)
    root = tmp_path / "catalog"
    imported = intake.import_dataset(root, source, "bars", _bars_contract())

    scoped = intake.check_dataset(
        root,
        "bars",
        version=imported["version"]["id"],
        purpose="daily_bars_research",
        columns=["symbol", "close"],
        symbols=["000002"],
        start="2026-01-01",
        end="2026-01-31",
    )

    assert scoped["allowed"] is False
    assert any(issue["rule"] == "type_date" for issue in scoped["issues"])
    with pytest.raises(intake.ResearchIntakeError, match="YYYY-MM-DD"):
        intake.check_dataset(
            root,
            "bars",
            version=imported["version"]["id"],
            purpose="daily_bars_research",
            start="01/02/2026",
        )


@pytest.mark.parametrize("primary_key", [[], ["volume"]])
def test_daily_strict_use_checks_natural_key_within_actual_scope(tmp_path, primary_key):
    source = tmp_path / "bars.csv"
    rows = _rows()
    rows.append(
        {
            **rows[0],
            "close_px": "10.75",
            "volume_shares": "101",
        }
    )
    _write_csv(source, rows)
    contract = _bars_contract(primary_key=primary_key)
    root = tmp_path / "catalog"
    imported = intake.import_dataset(root, source, "bars", contract)

    assert imported["version"]["status"] == "ready"
    inside = intake.check_dataset(
        root,
        "bars",
        purpose="daily_bars_research",
        start="2026-01-02",
        end="2026-01-02",
    )
    outside = intake.check_dataset(
        root,
        "bars",
        purpose="daily_bars_research",
        start="2026-01-05",
        end="2026-01-05",
    )

    assert inside["allowed"] is False
    conflict = next(
        issue for issue in inside["issues"] if issue["rule"] == "daily_bars_natural_key_conflict"
    )
    assert conflict["affected_count"] == 2
    assert conflict["column"] == "symbol,date"
    assert outside["allowed"] is True


@pytest.mark.parametrize(
    ("field", "declared_type"),
    [("date", "string"), ("symbol", "number"), ("close", "string")],
)
def test_daily_strict_use_requires_canonical_financial_types(tmp_path, field, declared_type):
    source = tmp_path / "bars.csv"
    rows = _rows()
    contract = _bars_contract()
    contract["types"][field] = declared_type
    if field == "date":
        rows[0]["trading_day"] = "01/02/2026"
        rows[1]["trading_day"] = "01/05/2026"
        contract["formats"].pop("date")
    _write_csv(source, rows)
    root = tmp_path / "catalog"
    imported = intake.import_dataset(root, source, "bars", contract)

    assert imported["version"]["status"] == "ready"
    strict = intake.check_dataset(root, "bars", purpose="daily_bars_research")
    assert strict["allowed"] is False
    assert any(
        issue["rule"] == "purpose_required_type" and issue["column"] == field
        for issue in strict["issues"]
    )
    if field == "date":
        assert imported["report"]["coverage"]["start"] is None
        scoped = intake.check_dataset(
            root,
            "bars",
            purpose="daily_bars_research",
            start="2026-01-01",
            end="2026-01-31",
        )
        assert scoped["allowed"] is False
        assert scoped["scope"]["matched_rows"] == 0


def test_parquet_date_with_time_is_blocked_instead_of_truncated(tmp_path):
    source = tmp_path / "bars.parquet"
    frame = pd.DataFrame(_rows())
    frame["trading_day"] = pd.to_datetime(frame["trading_day"])
    frame.loc[0, "trading_day"] = pd.Timestamp("2026-01-02 12:30:00")
    frame.to_parquet(source, index=False)
    contract = _bars_contract()
    contract["input"] = {"format": "parquet"}

    imported = intake.import_dataset(tmp_path / "catalog", source, "bars", contract)

    assert imported["version"]["status"] == "blocked"
    assert any(issue["rule"] == "date_has_time" for issue in imported["report"]["issues"])


def test_history_without_available_at_never_passes_historical_factor_backtest(tmp_path):
    source = tmp_path / "history.csv"
    _write_csv(
        source,
        [
            {
                "ticker": "000001",
                "period": "2025-12-31",
                "roe": "0.12",
            }
        ],
    )
    contract = {
        "schema_version": intake.CONTRACT_SCHEMA_VERSION,
        "kind": "history",
        "input": {"format": "csv", "encoding": "utf-8", "delimiter": ","},
        "mapping": {"symbol": "ticker", "period_end": "period", "roe": "roe"},
        "types": {"symbol": "string", "period_end": "date", "roe": "number"},
        "formats": {"period_end": "%Y-%m-%d"},
        "primary_key": ["symbol", "period_end"],
        "metadata": {
            "source": "filings-export",
            "provider": "test-vendor",
            "units": {"roe": "ratio"},
            "timezone": "UTC",
            "availability": "latest_only",
        },
    }
    root = tmp_path / "catalog"
    imported = intake.import_dataset(root, source, "fundamentals", contract)

    assert (
        intake.check_dataset(
            root,
            "fundamentals",
            purpose="exploration",
        )["allowed"]
        is True
    )
    strict = intake.check_dataset(
        root,
        "fundamentals",
        purpose="historical_financial_factor_backtest",
    )
    assert strict["allowed"] is False
    assert any(issue["rule"] == "historical_available_at" for issue in strict["issues"])
    assert imported["version"]["status"] == "ready"


def test_history_strict_use_requires_period_end_date_type(tmp_path):
    source = tmp_path / "history.csv"
    _write_csv(
        source,
        [
            {
                "ticker": "000001",
                "period": "2025-12-31",
                "published": "2026-03-01T08:30:00+08:00",
                "roe": "0.12",
            }
        ],
    )
    contract = {
        "schema_version": intake.CONTRACT_SCHEMA_VERSION,
        "kind": "history",
        "input": {"format": "csv", "encoding": "utf-8", "delimiter": ","},
        "mapping": {
            "symbol": "ticker",
            "period_end": "period",
            "available_at": "published",
            "roe": "roe",
        },
        "types": {
            "symbol": "string",
            "period_end": "string",
            "available_at": "datetime",
            "roe": "number",
        },
        "formats": {"available_at": "%Y-%m-%dT%H:%M:%S%z"},
        "primary_key": ["symbol", "period_end"],
        "metadata": {
            "source": "filings-export",
            "provider": "test-vendor",
            "units": {"roe": "ratio"},
            "availability": "point_in_time",
        },
    }
    root = tmp_path / "catalog"
    imported = intake.import_dataset(root, source, "history", contract)

    assert imported["version"]["status"] == "ready"
    strict = intake.check_dataset(
        root,
        "history",
        purpose="historical_financial_factor_backtest",
    )
    assert strict["allowed"] is False
    assert any(
        issue["rule"] == "purpose_required_type" and issue["column"] == "period_end"
        for issue in strict["issues"]
    )


def test_naive_history_datetime_without_timezone_is_not_guessed(tmp_path):
    source = tmp_path / "history.csv"
    _write_csv(
        source,
        [
            {
                "ticker": "000001",
                "period": "2025-12-31",
                "published": "2026-03-01 08:30:00",
                "roe": "0.12",
            }
        ],
    )
    contract = {
        "schema_version": intake.CONTRACT_SCHEMA_VERSION,
        "kind": "history",
        "input": {"format": "csv", "encoding": "utf-8", "delimiter": ","},
        "mapping": {
            "symbol": "ticker",
            "period_end": "period",
            "available_at": "published",
            "roe": "roe",
        },
        "types": {
            "symbol": "string",
            "period_end": "date",
            "available_at": "datetime",
            "roe": "number",
        },
        "formats": {
            "period_end": "%Y-%m-%d",
            "available_at": "%Y-%m-%d %H:%M:%S",
        },
        "primary_key": ["symbol", "period_end"],
        "metadata": {
            "source": "filings-export",
            "provider": "test-vendor",
            "units": {"roe": "ratio"},
            "availability": "point_in_time",
        },
    }
    imported = intake.import_dataset(tmp_path / "catalog", source, "history", contract)

    assert imported["version"]["status"] == "blocked"
    assert any(
        issue["rule"] == "explicit_datetime_timezone" for issue in imported["report"]["issues"]
    )


def test_ambiguous_local_datetime_is_reported_instead_of_silently_lost(tmp_path):
    source = tmp_path / "history.csv"
    _write_csv(
        source,
        [
            {
                "ticker": "000001",
                "period": "2025-12-31",
                "published": "2026-11-01 01:30:00",
                "roe": "0.12",
            }
        ],
    )
    contract = {
        "schema_version": intake.CONTRACT_SCHEMA_VERSION,
        "kind": "history",
        "input": {"format": "csv", "encoding": "utf-8", "delimiter": ","},
        "mapping": {
            "symbol": "ticker",
            "period_end": "period",
            "available_at": "published",
            "roe": "roe",
        },
        "types": {
            "symbol": "string",
            "period_end": "date",
            "available_at": "datetime",
            "roe": "number",
        },
        "formats": {
            "period_end": "%Y-%m-%d",
            "available_at": "%Y-%m-%d %H:%M:%S",
        },
        "primary_key": ["symbol", "period_end"],
        "metadata": {
            "source": "filings-export",
            "provider": "test-vendor",
            "units": {"roe": "ratio"},
            "timezone": "America/New_York",
            "availability": "point_in_time",
        },
    }

    imported = intake.import_dataset(tmp_path / "catalog", source, "history", contract)

    assert imported["version"]["status"] == "blocked"
    assert any(issue["rule"] == "datetime_localization" for issue in imported["report"]["issues"])


def test_tampered_raw_and_normalized_are_rejected(tmp_path):
    source = tmp_path / "bars.csv"
    _write_csv(source, _rows())
    root = tmp_path / "catalog"
    version = intake.import_dataset(root, source, "bars", _bars_contract())["version"]
    raw_path = root / "versions" / version["id"] / version["raw"]["path"]
    raw_path.write_bytes(raw_path.read_bytes() + b"tampered")
    with pytest.raises(intake.IntegrityError, match="raw"):
        intake.read_dataset(root, "bars")

    root2 = tmp_path / "catalog2"
    version2 = intake.import_dataset(root2, source, "bars", _bars_contract())["version"]
    normalized = root2 / "versions" / version2["id"] / "normalized.parquet"
    normalized.write_bytes(normalized.read_bytes() + b"tampered")
    with pytest.raises(intake.IntegrityError, match="normalized"):
        intake.read_dataset(root2, "bars")


def test_csv_parquet_equivalence_and_diff_counts(tmp_path):
    csv_source = tmp_path / "old.csv"
    _write_csv(csv_source, _rows())
    root = tmp_path / "catalog"
    old = intake.import_dataset(root, csv_source, "bars", _bars_contract())["version"]

    parquet_source = tmp_path / "same.parquet"
    pd.DataFrame(_rows()).to_parquet(parquet_source, index=False)
    parquet_contract = _bars_contract()
    parquet_contract["input"] = {"format": "parquet"}
    same = intake.import_dataset(root, parquet_source, "bars", parquet_contract)["version"]
    equivalent = intake.diff_versions(root, "bars", old["id"], same["id"])
    assert equivalent["rows"] == {"added": 0, "removed": 0, "changed": 0, "unchanged": 2}

    changed_source = tmp_path / "changed.csv"
    changed_rows = _rows()
    changed_rows[0]["close_px"] = "10.75"
    changed_rows.pop(1)
    changed_rows.append(
        {
            "ticker": "000003",
            "trading_day": "2026-01-06",
            "open_px": "30",
            "high_px": "31",
            "low_px": "29",
            "close_px": "30.5",
            "volume_shares": "300",
        }
    )
    _write_csv(changed_source, changed_rows)
    changed = intake.import_dataset(root, changed_source, "bars", _bars_contract())["version"]
    diff = intake.diff_versions(root, "bars", old["id"], changed["id"])
    assert diff["rows"] == {"added": 1, "removed": 1, "changed": 1, "unchanged": 0}
    assert diff["coverage"]["new"]["symbols"] == 2


def test_script_cli_runs_without_installed_package(tmp_path):
    source = tmp_path / "bars.csv"
    _write_csv(source, _rows())
    module_path = Path(intake.__file__).resolve()
    completed = subprocess.run(
        [
            sys.executable,
            str(module_path),
            "inspect-source",
            "--source",
            str(source),
            "--format",
            "csv",
            "--encoding",
            "utf-8",
            "--delimiter",
            ",",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "inspect-source"
    assert payload["data"]["rows"][0]["ticker"] == "000001"


def test_copied_snapshot_has_catalog_free_cli_read(tmp_path):
    source = tmp_path / "bars.csv"
    _write_csv(source, _rows())
    root = tmp_path / "catalog"
    imported = intake.import_dataset(root, source, "bars", _bars_contract())
    version_id = imported["version"]["id"]
    frozen = tmp_path / "frozen-version"
    shutil.copytree(root / "versions" / version_id, frozen)
    module_path = Path(intake.__file__).resolve()
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [
            sys.executable,
            str(module_path),
            "read-snapshot",
            "--snapshot",
            str(frozen),
            "--purpose",
            "daily_bars_research",
            "--limit",
            "0",
            "--max-rows",
            "2",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    payload = json.loads(completed.stdout)
    assert payload["ok"] is True
    assert payload["data"]["version_id"] == version_id
    assert payload["data"]["matched_rows"] == 2
    assert payload["data"]["complete"] is True


def test_direct_cli_import_returns_structured_version_and_receipt(tmp_path):
    source = tmp_path / "bars.csv"
    _write_csv(source, _rows())
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(_bars_contract()), encoding="utf-8")
    module_path = Path(intake.__file__).resolve()
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [
            sys.executable,
            str(module_path),
            "import",
            "--root",
            str(tmp_path / "catalog"),
            "--source",
            str(source),
            "--name",
            "bars",
            "--contract",
            str(contract_path),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    payload = json.loads(completed.stdout)
    assert payload["ok"] is True
    assert payload["data"]["receipt"]["status"] == "succeeded"
    assert payload["data"]["version"]["status"] == "ready"
