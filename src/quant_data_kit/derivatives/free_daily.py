"""Cboe VX contract CSV and NSE index F&O UDiFF adapters."""

import io
import zipfile
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from .free_common import MAX_BYTES, PublicSourceError, csv_rows, daily_time
from .models import Contract, Quote, decimal

CBOE = "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/"
NSE = "https://nsearchives.nseindia.com/content/fo/"
INDEX_PRODUCTS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}


def cboe_vix(resources, selected, first, last, *, tick, margins):
    contracts, quotes, skipped = [], [], []
    for expiration in selected:
        expiry_day = date.fromisoformat(expiration)
        filename = f"VX_{expiration}.csv"
        rows = csv_rows(
            resources.read(filename, CBOE + filename),
            [
                "Trade Date",
                "Futures",
                "Open",
                "High",
                "Low",
                "Close",
                "Settle",
                "Total Volume",
                "Open Interest",
            ],
        )
        instrument = f"CFE:VX:{expiration}"
        contract_quotes = []
        seen = set()
        for row in rows:
            day = date.fromisoformat(row["Trade Date"])
            if not first <= day <= last:
                continue
            if day in seen:
                raise ValueError("duplicate Cboe contract session")
            seen.add(day)
            month = [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ][expiry_day.month - 1]
            label = f"({month} {expiry_day.year})"
            if label not in row["Futures"]:
                raise ValueError("Cboe contract month does not match selected expiry")
            volume = decimal(row["Total Volume"])
            prices = [decimal(row[k]) for k in ("Open", "High", "Low", "Close")]
            if volume == 0:
                skipped.append(
                    {
                        "symbol": instrument,
                        "session": day.isoformat(),
                        "reason": "zero trading volume; reference/settlement-only row excluded",
                    }
                )
                continue
            if min(prices) <= 0:
                raise ValueError("Cboe traded bar has missing/nonpositive OHLC; no fabricated fill")
            at, available = daily_time(day, "America/Chicago", 16)
            contract_quotes.append(
                Quote(
                    instrument,
                    at,
                    available,
                    day.isoformat(),
                    *prices,
                    volume,
                    open_interest=row["Open Interest"],
                    settlement=row["Settle"],
                )
            )
        if not contract_quotes:
            raise ValueError(f"no traded bars for selected Cboe expiry {expiration}")
        listed = min(q.at for q in contract_quotes) - timedelta(days=1)
        expiry = datetime.combine(expiry_day, time(8), ZoneInfo("America/Chicago"))
        contracts.append(
            Contract(
                instrument,
                "VX-" + expiration,
                "VX",
                "CFE",
                "USD",
                "future",
                Decimal(1000),
                tick,
                listed,
                expiry,
                expiry,
                listed,
                "America/Chicago",
                "cash",
                initial_margin=margins[0],
                maintenance_margin=margins[1],
                rules_source="Cboe VX specifications; explicit expiry selection; first-observed lifecycle and user tick/margin assumptions",
            )
        )
        quotes.extend(contract_quotes)
    return (
        contracts,
        quotes,
        {
            "excluded_rows": skipped,
            "unavailable_days": [],
            "contract_rules_url": "https://www.cboe.com/tradable-products/vix/vix-futures/specifications",
            "date_basis": "Chicago daily session; publication assumed next local day 09:00; not PIT evidence",
        },
    )


def nse_csv(raw):
    if not raw.startswith(b"PK"):
        raise ValueError("NSE response must be an official ZIP, not an HTML/error page")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        items = archive.infolist()
        if (
            len(items) != 1
            or not items[0].filename.endswith(".csv")
            or items[0].file_size > MAX_BYTES
            or items[0].flag_bits & 1
        ):
            raise ValueError("NSE ZIP must contain one unencrypted CSV within 32 MiB")
        item = items[0]
        if (
            "/" in item.filename
            or "\\" in item.filename
            or item.file_size > max(1, item.compress_size) * 200
        ):
            raise ValueError("unsafe or excessive NSE archive expansion")
        return archive.read(item)


def nse_fo(resources, selected, first, last, *, kind, tick, margins):
    quotes, definitions, missing, skipped = [], {}, [], []
    day = first
    expected_type = "IDO" if kind == "option" else "IDF"
    required = [
        "TradDt",
        "Src",
        "Sgmt",
        "FinInstrmTp",
        "FinInstrmId",
        "FinInstrmNm",
        "TckrSymb",
        "FininstrmActlXpryDt",
        "NewBrdLotQty",
        "OpnPric",
        "HghPric",
        "LwPric",
        "ClsPric",
        "SttlmPric",
        "TtlTradgVol",
        "OpnIntrst",
        "UndrlygPric",
        "StrkPric",
        "OptnTp",
    ]
    while day <= last:
        filename = f"BhavCopy_NSE_FO_0_0_0_{day:%Y%m%d}_F_0000.csv.zip"
        try:
            raw = resources.read(filename, NSE + filename)
        except PublicSourceError as exc:
            if exc.status != 404:
                raise
            missing.append(day.isoformat())
            day += timedelta(days=1)
            continue
        rows = csv_rows(nse_csv(raw), required)
        seen = set()
        for row in rows:
            name = row["FinInstrmNm"]
            if name not in selected:
                continue
            if name in seen:
                raise ValueError("duplicate NSE contract session")
            seen.add(name)
            if row["TradDt"] != day.isoformat() or row["Src"] != "NSE" or row["Sgmt"] != "FO":
                raise ValueError("NSE source/date/segment does not match request")
            if row["FinInstrmTp"] != expected_type or row["TckrSymb"] not in INDEX_PRODUCTS:
                raise ValueError("adapter supports selected NSE index futures/options only")
            expiry_day = date.fromisoformat(row["FininstrmActlXpryDt"])
            if last >= expiry_day:
                raise ValueError("NSE sample must end before expiry; final fixing is separate")
            lot = decimal(row["NewBrdLotQty"])
            if lot <= 0 or lot != int(lot):
                raise ValueError("NSE historical lot size must be a positive integer")
            definition = {
                k: row[k]
                for k in (
                    "FinInstrmId",
                    "TckrSymb",
                    "FininstrmActlXpryDt",
                    "NewBrdLotQty",
                    "StrkPric",
                    "OptnTp",
                )
            }
            if name in definitions and definitions[name] != definition:
                raise ValueError("NSE contract rules changed within window; split the input bundle")
            definitions[name] = definition
            volume = decimal(row["TtlTradgVol"])
            if volume == 0:
                skipped.append(
                    {
                        "symbol": name,
                        "session": day.isoformat(),
                        "reason": "zero-volume daily bar excluded",
                    }
                )
                continue
            prices = [decimal(row[k]) for k in ("OpnPric", "HghPric", "LwPric", "ClsPric")]
            if min(prices) <= 0:
                raise ValueError("NSE traded bar has nonpositive OHLC")
            at, available = daily_time(day, "Asia/Kolkata", 15, 30)
            underlying = decimal(row["UndrlygPric"]) if row["UndrlygPric"] else None
            if kind == "option" and (underlying is None or underlying <= 0):
                raise ValueError("NSE option row lacks a positive source underlying price")
            quotes.append(
                Quote(
                    "NSE:" + name,
                    at,
                    available,
                    day.isoformat(),
                    *prices,
                    volume,
                    open_interest=decimal(row["OpnIntrst"]) / lot,
                    settlement=row["SttlmPric"] or None,
                    underlying_price=underlying,
                )
            )
        day += timedelta(days=1)
    contracts = []
    for name in selected:
        observed = [q for q in quotes if q.instrument_id == "NSE:" + name]
        if not observed:
            raise ValueError(f"no traded observations for requested NSE symbol {name}")
        row = definitions[name]
        expiry_day = date.fromisoformat(row["FininstrmActlXpryDt"])
        expiry = datetime.combine(expiry_day, time(15, 30), ZoneInfo("Asia/Kolkata"))
        listed = min(q.at for q in observed) - timedelta(days=1)
        option = {}
        if kind == "option":
            if row["OptnTp"] not in {"CE", "PE"}:
                raise ValueError("NSE index option must declare CE or PE")
            option = {
                "underlying": row["TckrSymb"],
                "option_right": "call" if row["OptnTp"] == "CE" else "put",
                "strike": row["StrkPric"],
                "exercise_style": "european",
            }
        contracts.append(
            Contract(
                "NSE:" + name,
                name,
                row["TckrSymb"],
                "NSE",
                "INR",
                kind,
                row["NewBrdLotQty"],
                tick,
                listed,
                expiry,
                expiry,
                listed,
                "Asia/Kolkata",
                "cash",
                initial_margin=margins[0],
                maintenance_margin=margins[1],
                rules_source="NSE UDiFF actual expiry and historical lot; index-only cash settlement; explicit user tick/margin assumptions",
                **option,
            )
        )
    return (
        contracts,
        quotes,
        {
            "excluded_rows": skipped,
            "unavailable_days": missing,
            "definitions": definitions,
            "volume_unit": "contracts; source open interest divided by historical NewBrdLotQty",
            "date_basis": "NSE TradDt, assumed close 15:30 IST and publication next local day 09:00; not PIT certified",
            "missing_days_note": "HTTP 404 dates are disclosed, not asserted to be exchange holidays or complete coverage",
        },
    )
