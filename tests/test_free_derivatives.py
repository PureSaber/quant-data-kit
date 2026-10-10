"""Official-shaped synthetic responses; no downloaded market files in Git."""

import csv
import io
import json
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from quant_data_kit.derivatives import load_bundle
from quant_data_kit.derivatives.free_common import PublicSourceError, Resources, validate_request
from quant_data_kit.derivatives.free_daily import nse_csv
from quant_data_kit.derivatives.free_deribit import current_snapshot
from quant_data_kit.derivatives.free_sources import collect_free


def csv_bytes(rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode()


def cboe(**changes):
    return dict(
        {
            "Trade Date": "2025-01-08",
            "Futures": "G (Feb 2025)",
            "Open": "18",
            "High": "20",
            "Low": "17",
            "Close": "19",
            "Settle": "19.1",
            "Total Volume": "100",
            "Open Interest": "200",
        },
        **changes,
    )


def collect(tmp_path, provider="cboe-vix", kind="future", selection="2025-02-19", **kwargs):
    return collect_free(
        tmp_path / "bundle",
        provider=provider,
        kind=kind,
        selection=selection,
        start="2025-01-08",
        end="2025-01-08",
        rights_note="synthetic test responses",
        **kwargs,
    )


def test_cboe_units_causal_availability_and_mode_guard(tmp_path):
    b = collect(tmp_path, fetch=lambda _: csv_bytes([cboe()]))
    assert b.contracts[0].multiplier == 1000
    assert b.quotes[0].settlement == Decimal("19.1")
    assert b.asof(b.quotes[0].at) == {}
    assert b.allowed_modes == ["analysis"]
    with pytest.raises(ValueError, match="permits"):
        b.require_mode("replay")
    source = b.manifest["source"]
    assert source["resources"][0]["sha256"]
    assert source["suggested_config"] == {"mode": "analysis", "product": "VX"}


def test_cboe_official_file_import_and_explicit_margin(tmp_path):
    (tmp_path / "VX_2025-02-19.csv").write_bytes(csv_bytes([cboe()]))
    b = collect(
        tmp_path,
        files_directory=tmp_path,
        initial_margin="10000",
        maintenance_margin="8000",
        fetch=lambda _: pytest.fail("offline request reached network"),
    )
    b.require_mode("replay")
    assert b.contracts[0].initial_margin == 10000


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"High": "1"}, "OHLC"),
        ({"Futures": "H (Mar 2025)"}, "month"),
        ({"Open": "0"}, "nonpositive"),
        ({"Total Volume": "0"}, "no traded"),
        ({"Total Volume": "-1"}, "negative"),
    ],
)
def test_cboe_rejects_invalid_traded_rows(tmp_path, changes, match):
    with pytest.raises(ValueError, match=match):
        collect(tmp_path, fetch=lambda _: csv_bytes([cboe(**changes)]))
    assert not (tmp_path / "bundle").exists()


def test_zero_volume_bad_ohlc_is_disclosed_not_fabricated(tmp_path):
    rows = [
        cboe(),
        cboe(
            **{
                "Trade Date": "2025-01-09",
                "Open": "0",
                "High": "0",
                "Low": "30",
                "Close": "0",
                "Total Volume": "0",
            }
        ),
    ]
    b = collect_free(
        tmp_path / "b",
        provider="cboe-vix",
        kind="future",
        selection="2025-02-19",
        start="2025-01-08",
        end="2025-01-09",
        rights_note="test",
        fetch=lambda _: csv_bytes(rows),
    )
    assert len(b.quotes) == 1
    assert len(b.manifest["source"]["quality"]["excluded_rows"]) == 1


def nse(**changes):
    return dict(
        {
            "TradDt": "2025-01-08",
            "Src": "NSE",
            "Sgmt": "FO",
            "FinInstrmTp": "IDO",
            "FinInstrmId": "100",
            "FinInstrmNm": "NIFTY25JAN23500PE",
            "TckrSymb": "NIFTY",
            "FininstrmActlXpryDt": "2025-01-30",
            "NewBrdLotQty": "25",
            "OpnPric": "225",
            "HghPric": "288",
            "LwPric": "194",
            "ClsPric": "208",
            "SttlmPric": "208",
            "TtlTradgVol": "100",
            "OpnIntrst": "2500",
            "UndrlygPric": "23688",
            "StrkPric": "23500",
            "OptnTp": "PE",
        },
        **changes,
    )


def zipped(rows, name="official.csv"):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(name, csv_bytes(rows))
    return stream.getvalue()


def test_nse_historical_lot_and_open_interest_units(tmp_path):
    b = collect(tmp_path, "nse-fo", "option", "NIFTY25JAN23500PE", fetch=lambda _: zipped([nse()]))
    assert b.contracts[0].multiplier == 25
    assert b.contracts[0].currency == "INR"
    assert b.quotes[0].volume == b.quotes[0].open_interest == 100
    assert b.quotes[0].underlying_price == 23688


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"TradDt": "2025-01-07"}, "date"),
        ({"FinInstrmTp": "STO"}, "index"),
        ({"NewBrdLotQty": "0"}, "lot"),
        ({"UndrlygPric": ""}, "underlying"),
        ({"OptnTp": "BAD"}, "CE or PE"),
    ],
)
def test_nse_rejects_unsupported_or_incomplete_rules(tmp_path, changes, match):
    with pytest.raises(ValueError, match=match):
        collect(
            tmp_path,
            "nse-fo",
            "option",
            "NIFTY25JAN23500PE",
            fetch=lambda _: zipped([nse(**changes)]),
        )


def test_nse_lot_change_requires_split(tmp_path):
    def fetch(url):
        return (
            zipped([nse()])
            if "20250108" in url
            else zipped([nse(TradDt="2025-01-09", NewBrdLotQty="75")])
        )

    with pytest.raises(ValueError, match="rules changed"):
        collect_free(
            tmp_path / "b",
            provider="nse-fo",
            kind="option",
            selection="NIFTY25JAN23500PE",
            start="2025-01-08",
            end="2025-01-09",
            rights_note="test",
            fetch=fetch,
        )


def test_nse_discloses_404_but_never_bypasses_403(tmp_path):
    def fetch(url):
        if "20250108" in url:
            return zipped([nse()])
        raise PublicSourceError(404)

    b = collect_free(
        tmp_path / "b",
        provider="nse-fo",
        kind="option",
        selection="NIFTY25JAN23500PE",
        start="2025-01-08",
        end="2025-01-09",
        rights_note="test",
        fetch=fetch,
    )
    assert b.manifest["source"]["quality"]["unavailable_days"] == ["2025-01-09"]
    calls = []

    def blocked(url):
        calls.append(url)
        raise PublicSourceError(403)

    with pytest.raises(PublicSourceError):
        collect(tmp_path, "nse-fo", "option", "NIFTY25JAN23500PE", fetch=blocked)
    assert len(calls) == 1


@pytest.mark.parametrize("raw", [b"<html>blocked</html>", zipped([nse()], "../escape.csv")])
def test_unsafe_nse_archive(raw):
    with pytest.raises(ValueError):
        nse_csv(raw)


NAME = "BTC-31JAN25-100000-C"
INFO = {
    "instrument_name": NAME,
    "kind": "option",
    "base_currency": "BTC",
    "quote_currency": "BTC",
    "settlement_currency": "BTC",
    "counter_currency": "USD",
    "option_type": "call",
    "contract_size": 1,
    "strike": 100000,
    "creation_timestamp": 1729756811000,
    "expiration_timestamp": 1738310400000,
}


def trade(seq=1, **changes):
    return dict(
        {
            "instrument_name": NAME,
            "trade_seq": seq,
            "timestamp": 1736298000000 + seq,
            "index_price": 100000,
            "price": 0.04,
            "amount": 0.5,
        },
        **changes,
    )


def rpc(result):
    return json.dumps({"result": result}).encode()


def test_deribit_pagination_and_usd_projection_never_replay(tmp_path):
    calls = []

    def fetch(url):
        calls.append(url)
        if "get_instrument?" in url:
            return rpc(INFO)
        if "start_seq" not in url:
            return rpc({"trades": [trade()], "has_more": True})
        assert parse_qs(urlparse(url).query)["start_seq"] == ["2"]
        return rpc({"trades": [trade(2, price=0.05)], "has_more": False})

    b = collect(tmp_path, "deribit", "option", NAME, source_mode="trades", fetch=fetch)
    assert len(calls) == 3
    assert b.quotes[0].close == 5000 and b.quotes[0].volume == 1
    assert b.allowed_modes == ["analysis"] and b.contracts[0].currency == "USD"
    path = b.root / "manifest.json"
    m = json.loads(path.read_text())
    m["source"]["allowed_modes"] = ["analysis", "replay"]
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match="analysis"):
        load_bundle(b.root)


def test_same_millisecond_trade_order_is_normalized(tmp_path):
    rows = [trade(2, timestamp=1736298000000), trade(1, timestamp=1736298000000)]

    def fetch(url):
        return rpc(INFO if "get_instrument?" in url else {"trades": rows, "has_more": False})

    b = collect(tmp_path, "deribit", "option", NAME, source_mode="trades", fetch=fetch)
    assert b.quotes[0].volume == 1


@pytest.mark.parametrize(
    "rows",
    [
        [trade(), trade(3)],
        [trade(), trade()],
        [],
        [trade(index_price=0)],
        [trade(instrument_name="ETH-bad")],
    ],
)
def test_deribit_rejects_gaps_empty_and_bad_units(tmp_path, rows):
    def fetch(url):
        return rpc(INFO if "get_instrument?" in url else {"trades": rows, "has_more": False})

    with pytest.raises(ValueError):
        collect(tmp_path, "deribit", "option", NAME, source_mode="trades", fetch=fetch)
    assert not (tmp_path / "bundle").exists()


@pytest.mark.parametrize("age,accepted", [(0, True), (121, False), (-6, False)])
def test_snapshot_is_labeled_and_fresh(age, accepted):
    now = datetime(2025, 1, 8, 12, tzinfo=timezone.utc)

    def fetch(url):
        if "get_instrument?" in url:
            return rpc(INFO)
        if "get_index_price?" in url:
            return rpc({"index_price": 100000})
        return rpc(
            [
                {
                    "instrument_name": NAME,
                    "creation_timestamp": int(now.timestamp() * 1000) - age * 1000,
                    "mark_price": 0.04,
                    "open_interest": 50,
                }
            ]
        )

    if not accepted:
        with pytest.raises(ValueError, match="stale/future"):
            current_snapshot(Resources(fetch=fetch), [NAME], tick=Decimal(".05"), now=now)
    else:
        _, quotes, quality = current_snapshot(
            Resources(fetch=fetch), [NAME], tick=Decimal(".05"), now=now
        )
        assert quotes[0].close == 4000 and quotes[0].volume == 0
        assert quality["allowed_modes"] == ["analysis"]
        assert "not executed trades" in quality["price_basis"]


@pytest.mark.parametrize(
    "provider,kind,selection,mode",
    [
        ("cboe-vix", "option", "2025-02-19", "daily"),
        ("deribit", "option", NAME, "daily"),
        ("deribit", "option", "https://evil.test", "trades"),
        ("nse-fo", "future", "x", "snapshot"),
    ],
)
def test_request_boundaries(provider, kind, selection, mode):
    with pytest.raises(ValueError):
        validate_request(provider, kind, selection, "2025-01-08", "2025-01-08", mode)
