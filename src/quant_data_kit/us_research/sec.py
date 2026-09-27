"""SEC snapshots and accession-level facts without backdating restatements."""

from __future__ import annotations

import itertools
import json
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd

from .prices import sha256, utc


def download_sec(cik: str, destination: str | Path, user_agent: str) -> Path:
    """Fetch companyfacts and all submissions pages with a real contact identity."""
    if not re.search(r"[^\s@]+@[^\s@]+\.[^\s@]+", user_agent):
        raise ValueError("SEC_USER_AGENT must contain a real contact email")
    if not re.fullmatch(r"\d{1,10}", str(cik)):
        raise ValueError("CIK must contain 1 to 10 digits")
    import requests

    cik = str(cik).zfill(10)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema": "puresaber.sec-snapshot/1",
        "cik": cik,
        "observed_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "files": {},
    }
    session = requests.Session()
    session.headers["User-Agent"] = user_agent

    def fetch(relative: str, name: str):
        time.sleep(0.25)
        response = session.get("https://data.sec.gov/" + relative, timeout=60)
        response.raise_for_status()
        payload = response.json()
        target = destination / name
        target.write_bytes(response.content)
        manifest["files"][name] = {"url": response.url, "sha256": sha256(target)}
        return payload

    try:
        submissions = fetch(f"submissions/CIK{cik}.json", "submissions.json")
        for item in submissions["filings"].get("files", []):
            name = item["name"]
            if not re.fullmatch(r"CIK\d{10}-submissions-\d+\.json", name):
                raise ValueError("unexpected SEC archive name")
            fetch("submissions/" + name, name)
        fetch(f"api/xbrl/companyfacts/CIK{cik}.json", "companyfacts.json")
        (destination / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    finally:
        session.close()
    return destination


def read_sec_snapshot(path: str | Path) -> tuple[dict, list[dict], dict]:
    path = Path(path)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema") != "puresaber.sec-snapshot/1":
        raise ValueError("unsupported SEC snapshot")
    records = {}
    for name, entry in manifest["files"].items():
        if Path(name).name != name or "/" in name or "\\" in name:
            raise ValueError("invalid SEC snapshot path")
        if sha256(path / name) != entry["sha256"]:
            raise ValueError(f"SEC hash mismatch: {name}")
        records[name] = json.loads((path / name).read_text(encoding="utf-8"))
    submissions = [records["submissions.json"]["filings"]["recent"]]
    submissions += [
        value for name, value in records.items() if name.startswith("CIK") and "submissions" in name
    ]
    return records["companyfacts.json"], submissions, manifest


def filing_table(
    submissions: list[dict], instrument_id: str, *, lag_minutes: int = 5
) -> pd.DataFrame:
    if isinstance(lag_minutes, bool) or not isinstance(lag_minutes, int) or lag_minutes < 0:
        raise ValueError("lag_minutes must be a nonnegative integer")
    rows = []
    for page in submissions:
        columns = ("accessionNumber", "acceptanceDateTime", "form", "filingDate")
        if not all(name in page for name in columns):
            raise ValueError("SEC submissions missing acceptance timestamps")
        if len({len(page[name]) for name in columns}) != 1:
            raise ValueError("SEC submissions columns have different lengths")
        for accession, accepted, form, filed in zip(*(page[name] for name in columns), strict=True):
            at = utc(accepted, "SEC acceptanceDateTime")
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "accession": accession,
                    "accepted_at": at,
                    "available_at": at + pd.Timedelta(minutes=lag_minutes),
                    "form": form,
                    "filed": filed,
                    "source": "SEC EDGAR",
                }
            )
    result = pd.DataFrame(rows)
    if result.empty:
        raise ValueError("no SEC filings")
    result = result.drop_duplicates()
    if result.accession.duplicated().any():
        raise ValueError("conflicting SEC accession metadata")
    return result.sort_values(["available_at", "accession"]).reset_index(drop=True)


def fact_table(
    companyfacts: dict,
    filings: pd.DataFrame,
    *,
    concepts: tuple[str, ...] = (
        "Assets",
        "NetIncomeLoss",
        "NetCashProvidedByUsedInOperatingActivities",
    ),
    unit: str = "USD",
) -> tuple[pd.DataFrame, dict]:
    """Preserve each filing vintage and exact period; exclude unknown acceptance."""
    index = filings.set_index("accession")
    if index.index.duplicated().any() or filings.instrument_id.nunique() != 1:
        raise ValueError("one entity and unique accessions required")
    rows, missing = [], set()
    for concept in concepts:
        values = companyfacts.get("facts", {}).get("us-gaap", {}).get(concept, {})
        for fact in values.get("units", {}).get(unit, []):
            accession = fact["accn"]
            if accession not in index.index:
                missing.add(accession)
                continue
            filing = index.loc[accession]
            value = float(fact["val"])
            if not np.isfinite(value):
                raise ValueError("nonfinite SEC fact")
            end = pd.Timestamp(fact["end"])
            start = pd.Timestamp(fact["start"]) if "start" in fact else pd.NaT
            if end.date() > filing.accepted_at.date() or (pd.notna(start) and start > end):
                raise ValueError("SEC fact period is inconsistent with filing time")
            rows.append(
                {
                    "instrument_id": filing.instrument_id,
                    "concept": concept,
                    "unit": unit,
                    "period_start": start,
                    "period_end": end,
                    "value": value,
                    "accession": accession,
                    "form": filing.form,
                    "accepted_at": filing.accepted_at,
                    "available_at": filing.available_at,
                    "fiscal_year": fact.get("fy"),
                    "fiscal_period": fact.get("fp"),
                }
            )
    columns = [
        "instrument_id",
        "concept",
        "unit",
        "period_start",
        "period_end",
        "value",
        "accession",
        "form",
        "accepted_at",
        "available_at",
        "fiscal_year",
        "fiscal_period",
    ]
    result = pd.DataFrame(rows, columns=columns).drop_duplicates()
    keys = ["instrument_id", "concept", "unit", "period_start", "period_end", "accession"]
    if result.duplicated(keys).any():
        raise ValueError("ambiguous same-accession financial facts")
    return result, {
        "excluded_unknown_accessions": sorted(missing),
        "rows": len(result),
        "availability": "SEC acceptance plus configured processing lag",
    }


def facts_asof(facts: pd.DataFrame, at) -> pd.DataFrame:
    facts = facts.copy()
    facts["value"] = pd.to_numeric(facts.value, errors="raise")
    if not np.isfinite(facts.value).all():
        raise ValueError("nonfinite SEC fact")
    facts["period_start"] = pd.to_datetime(facts.period_start)
    facts["period_end"] = pd.to_datetime(facts.period_end)
    facts["available_at"] = pd.to_datetime(
        facts.available_at.map(lambda value: utc(value, "fact available_at")), utc=True
    )
    if "accepted_at" in facts:
        accepted = facts.accepted_at.map(lambda value: utc(value, "SEC accepted_at"))
        if (facts.available_at < accepted).any():
            raise ValueError("SEC fact cannot be known before acceptance")
    if (facts.period_end > facts.available_at.dt.tz_localize(None)).any():
        raise ValueError("SEC fact period cannot be in the future")
    visible = facts.loc[facts.available_at <= utc(at)].copy()
    keys = ["instrument_id", "concept", "unit", "period_start", "period_end"]
    versions = visible.groupby([*keys, "available_at"], dropna=False).value.nunique()
    if versions.gt(1).any():
        raise ValueError("ambiguous same-time SEC facts")
    return visible.sort_values(["available_at", "accession"]).drop_duplicates(keys, keep="last")


def annual_quality(facts: pd.DataFrame, at, *, max_age_days: int = 550) -> pd.DataFrame:
    """Annual net-income/assets and cash accruals; no mislabeled quarterly/TTM mixing."""
    visible = facts_asof(facts, at)
    visible["period_start"] = pd.to_datetime(visible.period_start)
    visible["period_end"] = pd.to_datetime(visible.period_end)
    results = []
    for instrument, data in visible.groupby("instrument_id"):
        income = data.loc[data.concept.eq("NetIncomeLoss") & data.unit.eq("USD")].copy()
        duration = (income.period_end - income.period_start).dt.days
        income = income.loc[duration.between(330, 380)].sort_values("period_end")
        if income.empty:
            continue
        latest = income.iloc[-1]
        if (utc(at).tz_localize(None) - latest.period_end).days > max_age_days:
            continue
        assets = data.loc[
            data.concept.eq("Assets")
            & data.unit.eq("USD")
            & data.period_start.isna()
            & data.period_end.eq(latest.period_end)
        ]
        cash = data.loc[
            data.concept.eq("NetCashProvidedByUsedInOperatingActivities")
            & data.unit.eq("USD")
            & data.period_start.eq(latest.period_start)
            & data.period_end.eq(latest.period_end)
        ]
        if len(assets) != 1 or len(cash) != 1 or assets.iloc[0].value <= 0:
            continue
        a, c = assets.iloc[0], cash.iloc[0]
        results.append(
            {
                "instrument_id": instrument,
                "period_end": latest.period_end,
                "roa_annual": latest.value / a.value,
                "accruals_annual": (latest.value - c.value) / a.value,
                "available_at": max(latest.available_at, a.available_at, c.available_at),
                "accessions": "|".join(sorted({latest.accession, a.accession, c.accession})),
            }
        )
    return pd.DataFrame(
        results,
        columns=[
            "instrument_id",
            "period_end",
            "roa_annual",
            "accruals_annual",
            "available_at",
            "accessions",
        ],
    )


def quarterly_facts(facts: pd.DataFrame, at) -> pd.DataFrame:
    """Independent fiscal quarters, not calendar quarters or blindly divided YTD.

    53-week fiscal years are accepted through duration/contiguity checks. Fiscal
    year labels in companyfacts describe the FILING, so period boundaries govern
    derivation. Direct disclosed quarters take precedence over differences.
    Cross-accession differences remain flagged: restated YTD and old quarters
    are not automatically certified as comparable.
    """
    visible = facts_asof(facts, at).copy()
    visible["period_start"] = pd.to_datetime(visible.period_start)
    visible["period_end"] = pd.to_datetime(visible.period_end)
    rows = []
    for _, group in visible.groupby(["instrument_id", "concept", "unit"]):
        group = group.loc[group.period_start.notna()].sort_values("period_end")
        direct = {}
        for row in group.to_dict("records"):
            days = (row["period_end"] - row["period_start"]).days + 1
            if 60 <= days <= 110:
                key = (row["period_start"], row["period_end"])
                direct[key] = dict(
                    row,
                    method="reported_quarter",
                    accessions=row["accession"],
                    vintage_consistent=True,
                )
        derived = {}
        for row in group.to_dict("records"):
            duration = (row["period_end"] - row["period_start"]).days + 1
            if not 150 <= duration <= 380:
                continue
            prior = group.loc[
                (group.period_start == row["period_start"]) & (group.period_end < row["period_end"])
            ]
            if prior.empty:
                continue
            previous = prior.iloc[-1]
            start = previous.period_end + pd.Timedelta(days=1)
            days = (row["period_end"] - start).days + 1
            if not 60 <= days <= 110:
                continue
            key = (start, row["period_end"])
            if key in direct:
                continue
            candidate = dict(
                row,
                period_start=start,
                value=row["value"] - previous.value,
                method="cumulative_difference",
                available_at=max(row["available_at"], previous.available_at),
                accessions="|".join(sorted({row["accession"], previous.accession})),
                vintage_consistent=row["accession"] == previous.accession,
            )
            if key in derived and derived[key]["value"] != candidate["value"]:
                raise ValueError("conflicting cumulative derivations; reconcile SEC facts")
            derived[key] = candidate
        rows.extend([*direct.values(), *derived.values()])
    columns = [*facts.columns, "method", "accessions", "vintage_consistent"]
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["instrument_id", "concept", "unit", "period_end"]
    )


def ttm_facts(facts: pd.DataFrame, at, *, allow_mixed_vintages=False, max_age_days=200):
    """Four contiguous independent quarters, with unit and vintage lineage intact."""
    if type(allow_mixed_vintages) is not bool or type(max_age_days) is not int or max_age_days < 0:
        raise ValueError("explicit vintage policy and nonnegative max_age_days required")
    quarters = quarterly_facts(facts, at)
    result = []
    for key, rows in quarters.groupby(["instrument_id", "concept", "unit"]):
        last = rows.sort_values("period_end").tail(4)
        if len(last) != 4 or (not allow_mixed_vintages and not last.vintage_consistent.all()):
            continue
        records = last.to_dict("records")
        if any(
            b["period_start"] != a["period_end"] + pd.Timedelta(days=1)
            for a, b in itertools.pairwise(records)
        ):
            continue
        span = (records[-1]["period_end"] - records[0]["period_start"]).days + 1
        if not 330 <= span <= 380:
            continue
        if (utc(at).tz_localize(None) - records[-1]["period_end"]).days > max_age_days:
            continue
        result.append(
            {
                "instrument_id": key[0],
                "concept": key[1],
                "unit": key[2],
                "period_start": records[0]["period_start"],
                "period_end": records[-1]["period_end"],
                "value": sum(last.value),
                "available_at": max(last.available_at),
                "accessions": "|".join(sorted(set("|".join(last.accessions).split("|")))),
                "vintage_consistent": bool(last.vintage_consistent.all()),
            }
        )
    return pd.DataFrame(
        result,
        columns=[
            "instrument_id",
            "concept",
            "unit",
            "period_start",
            "period_end",
            "value",
            "available_at",
            "accessions",
            "vintage_consistent",
        ],
    )


def ttm_quality(facts, at, *, allow_mixed_vintages=False):
    flows = ttm_facts(facts, at, allow_mixed_vintages=allow_mixed_vintages)
    visible = facts_asof(facts, at)
    result = []
    for instrument, rows in flows.groupby("instrument_id"):
        income = rows.loc[rows.concept.eq("NetIncomeLoss") & rows.unit.eq("USD")]
        cash = rows.loc[
            rows.concept.eq("NetCashProvidedByUsedInOperatingActivities") & rows.unit.eq("USD")
        ]
        if len(income) != 1 or len(cash) != 1:
            continue
        i, c = income.iloc[0], cash.iloc[0]
        if i.period_start != c.period_start or i.period_end != c.period_end:
            continue
        assets = visible.loc[
            visible.instrument_id.eq(instrument)
            & visible.concept.eq("Assets")
            & visible.unit.eq("USD")
            & visible.period_start.isna()
            & pd.to_datetime(visible.period_end).eq(i.period_end)
        ]
        if len(assets) != 1 or assets.iloc[0].value <= 0:
            continue
        a = assets.iloc[0]
        result.append(
            {
                "instrument_id": instrument,
                "period_end": i.period_end,
                "roa_ttm": i.value / a.value,
                "accruals_ttm": (i.value - c.value) / a.value,
                "available_at": max(i.available_at, c.available_at, a.available_at),
                "accessions": "|".join(
                    sorted(set((i.accessions + "|" + c.accessions + "|" + a.accession).split("|")))
                ),
            }
        )
    return pd.DataFrame(
        result,
        columns=[
            "instrument_id",
            "period_end",
            "roa_ttm",
            "accruals_ttm",
            "available_at",
            "accessions",
        ],
    )
