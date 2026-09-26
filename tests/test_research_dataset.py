import json
from pathlib import Path

import pandas as pd
import pytest

from quant_data_kit.instrument_master import import_instrument_master
from quant_data_kit.providers.corporate_actions import normalize_etf_actions
from quant_data_kit.research_coverage import load_history
from quant_data_kit.research_dataset import (
    bind_instrument_master,
    build_dataset,
    inspect_dataset,
    load_research_snapshot,
    update_dataset,
)
from tests.test_instrument_master import _source as _master_source


def _prices(dates, closes, adjustment):
    rows = []
    for day, close in zip(dates, closes):
        rows.append(
            {
                "symbol": "510300",
                "date": day,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000_000.0,
                "amount": close * 1_000_000,
                "name": "ETF",
                "industry": None,
                "provider": "declared",
                "source": "local:test",
                "volume_unit": "share",
                "source_volume_unit": "share",
                "amount_unit": "CNY",
                "source_amount_unit": "CNY",
                "adjustment": adjustment,
            }
        )
    return pd.DataFrame(rows)


def _actions():
    return pd.DataFrame(
        [
            {
                "event_id": "real-source-document",
                "symbol": "510300",
                "announced_date": pd.Timestamp("2026-01-12"),
                "record_date": pd.Timestamp("2026-01-16"),
                "ex_date": pd.Timestamp("2026-01-19"),
                "pay_date": pd.Timestamp("2026-01-27"),
                "shares_available_date": pd.NaT,
                "cash_per_share": "1",
                "share_ratio": "1",
                "source": "local:evidence",
                "captured_at": "2026-01-22T00:00:00Z",
                "source_record": '{"document":"real"}',
            }
        ]
    )


def _source(root: Path, dates, raw_closes, adjusted_closes, *, actions=True):
    root.mkdir()
    _prices(dates, raw_closes, "none").to_parquet(root / "raw.parquet", index=False)
    _prices(dates, adjusted_closes, "qfq").to_parquet(root / "adjusted.parquet", index=False)
    pd.DataFrame(
        {
            "date": dates[1:],
            "benchmark_return": [0.01] * (len(dates) - 1),
        }
    ).to_parquet(root / "benchmark.parquet", index=False)
    pd.DataFrame(
        {
            "date": pd.DatetimeIndex(
                [
                    "2026-01-15",
                    "2026-01-16",
                    "2026-01-19",
                    "2026-01-20",
                    "2026-01-21",
                    "2026-01-22",
                    "2026-01-23",
                ]
            )
        }
    ).to_parquet(root / "calendar.parquet", index=False)
    if actions:
        _actions().to_parquet(root / "actions.parquet", index=False)


def _build(root: Path, source: Path, end="2026-01-21"):
    return build_dataset(
        root,
        symbols=["510300"],
        start="2026-01-15",
        end=end,
        captured_at="2026-01-22T00:00:00Z",
        source_dir=source,
        source_uri="file://declared-export",
        source_version="vendor-export-7",
        license_note="test fixture declaration",
    )


def test_local_build_is_immutable_asm_compatible_and_records_deferred_cash(tmp_path):
    source = tmp_path / "source"
    dates = pd.DatetimeIndex(["2026-01-15", "2026-01-16", "2026-01-19", "2026-01-20", "2026-01-21"])
    _source(source, dates, [10, 10, 9, 9.1, 9.2], [9, 9, 9, 9.1, 9.2])
    root = tmp_path / "dataset"
    manifest = _build(root, source)

    snapshot = root / "snapshots" / manifest["snapshot_id"]
    assert (snapshot / "raw.parquet").is_file()
    assert manifest["files"]["raw"]["file"] == "raw.parquet"
    assert manifest["files"]["actions"]["file"] == "actions.parquet"
    assert manifest["files"]["catalog"]["file"] == "catalog.csv"
    assert manifest["files"]["history"]["file"] == "history/history.parquet"
    assert manifest["validation"]["corporate_actions"]["deferred_payments"] == 1
    assert manifest["validation"]["adjustment_consistency"]["passed"]
    assert manifest["consumer_contract"]["asm_decision_workflow_load_inputs"] is True
    catalog = json.loads((root / "catalog.json").read_text(encoding="utf-8"))
    assert catalog["current_snapshot_id"] == manifest["snapshot_id"]
    loaded, frames = load_research_snapshot(root)
    assert loaded == manifest
    assert len(frames["history"]) == 4
    assert frames["history"]["available_at"].nunique() == 1
    assert frames["catalog"].loc[0, "product_type"] == "etf"
    assert frames["catalog"].loc[0, "asset_class"] == "etf"
    assert frames["catalog"].loc[0, "price_tick"] == pytest.approx(0.001)
    history_manifest, compatible_history = load_history(snapshot / "history")
    assert history_manifest["schema_version"] == "qdk.research-history/v1"
    assert len(compatible_history) == 4
    assert inspect_dataset(root)["validation"]["passed"]


def test_bind_historical_master_publishes_child_without_refetch(tmp_path):
    source = tmp_path / "source"
    dates = pd.DatetimeIndex(["2026-01-15", "2026-01-16", "2026-01-19", "2026-01-20", "2026-01-21"])
    _source(source, dates, [10, 10, 9, 9.1, 9.2], [9, 9, 9, 9.1, 9.2])
    root = tmp_path / "dataset"
    parent = _build(root, source)
    master = tmp_path / "master"
    import_instrument_master(_master_source(tmp_path / "master-source"), master)

    child = bind_instrument_master(
        root,
        master,
        captured_at="2026-09-26T00:00:00Z",
    )
    assert child["parent_snapshot_id"] == parent["snapshot_id"]
    assert child["update_evidence"]["market_data_refetched"] is False
    assert child["files"]["catalog"]["provider"] == "qdk.historical-instrument-master/v1"
    assert child["consumer_contract"]["instrument_master_availability"].startswith("official")
    _, frames = load_research_snapshot(root, child["snapshot_id"])
    assert frames["catalog"].loc[0, "available_at"] == "2023-02-17T08:00:00Z"

    refresh = tmp_path / "refresh"
    refresh_dates = pd.DatetimeIndex(
        ["2026-01-15", "2026-01-16", "2026-01-19", "2026-01-20", "2026-01-21", "2026-01-22"]
    )
    _source(refresh, refresh_dates, [10, 10, 9, 9.1, 9.2, 9.3], [9, 9, 9, 9.1, 9.2, 9.3])
    updated = update_dataset(
        root,
        end="2026-01-22",
        captured_at="2026-09-27T00:00:00Z",
        source_dir=refresh,
        source_uri="file://declared-refresh",
        source_version="vendor-export-8",
        license_note="test fixture declaration",
    )
    assert updated["parent_snapshot_id"] == child["snapshot_id"]
    assert updated["files"]["catalog"]["provider"] == "qdk.historical-instrument-master/v1"
    assert updated["validation"]["instrument_master"]["passed"] is True


def test_incremental_update_keeps_parent_and_records_overlap_revision(tmp_path):
    initial = tmp_path / "initial"
    initial_dates = pd.DatetimeIndex(["2026-01-15", "2026-01-16", "2026-01-19"])
    _source(initial, initial_dates, [10, 10, 9], [9, 9, 9])
    root = tmp_path / "dataset"
    first = _build(root, initial, end="2026-01-19")

    refresh = tmp_path / "refresh"
    refresh_dates = pd.DatetimeIndex(["2026-01-16", "2026-01-19", "2026-01-20", "2026-01-21"])
    _source(refresh, refresh_dates, [10, 9, 9.1, 9.2], [9, 9, 9.1, 9.2])
    second = update_dataset(
        root,
        end="2026-01-21",
        captured_at="2026-01-22T00:00:00Z",
        overlap_sessions=2,
        source_dir=refresh,
        source_uri="file://declared-refresh",
        source_version="vendor-export-8",
        license_note="test fixture declaration",
    )
    assert second["parent_snapshot_id"] == first["snapshot_id"]
    assert second["snapshot_id"] != first["snapshot_id"]
    assert second["update_evidence"]["fetched_from"] == "2026-01-16"
    assert len(json.loads((root / "catalog.json").read_text())["snapshots"]) == 2
    _, old = load_research_snapshot(root, first["snapshot_id"])
    _, current = load_research_snapshot(root, second["snapshot_id"])
    assert len(old["raw"]) == 3
    assert len(current["raw"]) == 5


def test_unexplained_adjustment_and_snapshot_mutation_fail_closed(tmp_path):
    source = tmp_path / "source"
    dates = pd.DatetimeIndex(["2026-01-15", "2026-01-16", "2026-01-19"])
    _source(source, dates, [10, 10, 9], [10, 10, 9], actions=False)
    # Create an unexplained factor jump, not a constant historical scale.
    adjusted = pd.read_parquet(source / "adjusted.parquet")
    adjusted.loc[adjusted.date < pd.Timestamp("2026-01-19"), "close"] = 9
    adjusted.loc[adjusted.date < pd.Timestamp("2026-01-19"), ["open", "high", "low"]] = 9
    adjusted.to_parquet(source / "adjusted.parquet", index=False)
    with pytest.raises(ValueError, match="real matching cashflow"):
        _build(tmp_path / "rejected", source, end="2026-01-19")

    valid = tmp_path / "valid"
    _source(
        valid,
        pd.DatetimeIndex(["2026-01-15", "2026-01-16", "2026-01-19"]),
        [10, 10, 9],
        [9, 9, 9],
    )
    root = tmp_path / "accepted"
    manifest = _build(root, valid, end="2026-01-19")
    snapshot = root / "snapshots" / manifest["snapshot_id"]
    manifest_path = snapshot / "manifest.json"
    original_manifest = manifest_path.read_bytes()
    changed_manifest = json.loads(original_manifest)
    changed_manifest["update_evidence"]["mode"] = "changed"
    manifest_path.write_text(json.dumps(changed_manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="manifest identity"):
        load_research_snapshot(root)
    manifest_path.write_bytes(original_manifest)
    raw = snapshot / "raw.parquet"
    raw.write_bytes(b"changed")
    with pytest.raises(ValueError, match="integrity"):
        load_research_snapshot(root)


def test_etf_action_normalizer_uses_source_announcement_and_per_ten_units():
    dividends = pd.DataFrame(
        [
            {
                "年份": "2026年",
                "权益登记日": "2026-01-16",
                "除息日": "2026-01-19",
                "每10份分红": "每10份派现金1.2300元",
                "分红发放日": "2026-01-27",
            }
        ]
    )
    announcements = pd.DataFrame(
        [
            {
                "基金代码": "510300",
                "公告标题": "沪深300ETF分红公告",
                "基金简称": "沪深300ETF",
                "公告日期": "2026-01-12",
                "报告ID": "AN-real",
            }
        ]
    )
    result = normalize_etf_actions(dividends, announcements, "510300", "2026-01-22T00:00:00Z")
    assert result.iloc[0].cash_per_share == "0.123"
    assert result.iloc[0].announced_date == pd.Timestamp("2026-01-12")
    assert result.iloc[0].pay_date == pd.Timestamp("2026-01-27")
    assert "AN-real" in result.iloc[0].source_record
    with pytest.raises(ValueError, match="lacks a prior announcement"):
        normalize_etf_actions(dividends, announcements.iloc[0:0], "510300", "now")


def test_coverage_distinguishes_leading_unavailability_from_later_gaps(tmp_path):
    source = tmp_path / "source"
    dates = pd.DatetimeIndex(["2026-01-16", "2026-01-19"])
    _source(source, dates, [10, 9], [9, 9])
    with pytest.raises(ValueError, match="pre_listing_or_provider_history_unavailable") as exc:
        _build(tmp_path / "dataset", source, end="2026-01-19")
    assert "missing_after_first_available_bar" in str(exc.value)
    assert "listing_date_basis" in str(exc.value)
