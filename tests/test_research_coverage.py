import pandas as pd
import pytest

from quant_data_kit.research_coverage import (
    asof_history,
    attach_history,
    import_history,
    load_history,
    preflight,
    validate_history,
)


def history():
    return pd.DataFrame(
        [
            {
                "domain": "fundamentals",
                "symbol": "000001",
                "field": "pe_ratio",
                "value": value,
                "effective_at": "2024-01-01T00:00:00Z",
                "available_at": available,
            }
            for value, available in [(10, "2024-01-03T00:00:00Z"), (20, "2024-01-10T00:00:00Z")]
        ]
    )


def test_import_revision_and_tampering(tmp_path):
    source = tmp_path / "history.csv"
    history().to_csv(source, index=False)
    root = tmp_path / "bundle"
    import_history(
        source, root, provider="test", source_uri="test://fixture", license_note="fixture"
    )
    _, frame = load_history(root)
    assert frame.symbol.iloc[0] == "000001"
    assert (
        asof_history(
            frame, as_of="2024-01-05T00:00Z", domain="fundamentals", field="pe_ratio"
        ).value.iloc[0]
        == "10"
    )
    with pytest.raises(FileExistsError):
        import_history(
            source, root, provider="test", source_uri="test://fixture", license_note="fixture"
        )
    (root / "history.parquet").write_bytes(b"bad")
    with pytest.raises(ValueError, match="hash"):
        load_history(root)


def test_publication_and_missing_time_are_not_inferred():
    h = validate_history(history())
    p = pd.DataFrame({"symbol": ["000001"] * 2, "date": ["2024-01-02", "2024-01-05"]})
    attached = attach_history(p, h, {"pe_ratio": "fundamentals"})
    assert pd.isna(attached.pe_ratio.iloc[0])
    assert attached.pe_ratio.iloc[1] == 10
    h.loc[0, "available_at"] = pd.NaT
    with pytest.raises(ValueError):
        validate_history(h)
    with pytest.raises(ValueError, match="time zones"):
        validate_history(history().assign(available_at="2024-01-01"))


def test_preflight_enumerates_unavailable_domains_and_sessions():
    cal = pd.DataFrame({"date": pd.bdate_range("2024-01-01", periods=10)})
    p = cal.assign(symbol="000001", close=10).iloc[:-1]
    report = preflight(
        p,
        cal,
        symbols=["000001"],
        start="2024-01-08",
        end="2024-01-12",
        requirements={"momentum": {"columns": ["close"], "warmup_bars": 2}},
        required_history={"tradable": "status"},
    )
    assert not report["passed"]
    assert {x["code"] for x in report["issues"]} == {
        "PRICE_OR_FEATURE_COVERAGE",
        "HISTORY_COVERAGE",
    }
