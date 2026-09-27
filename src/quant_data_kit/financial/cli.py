"""Offline financial-data operations with immutable JSON request/result artifacts."""

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from . import SCHEMA_VERSION
from .calendars import CalendarBook, PurposeCalendar
from .holdings import exposure_summary, look_through
from .lifecycle import lifecycle_asof, select_universe
from .macro import macro_asof
from .reconciliation import canonical, digest, discrepancies
from .status import permission_asof
from .units import normalize_trading_units


def run_request(request):
    if request.get("schema") != SCHEMA_VERSION:
        raise ValueError("unsupported financial request schema")
    operation = request["operation"]
    frame = pd.DataFrame(request.get("records", []))
    options = request.get("options", {})
    if operation == "lifecycle":
        result = lifecycle_asof(frame, **options).to_dict("records")
    elif operation == "universe":
        selected = select_universe(frame, **options)
        result = {k: sorted(v) for k, v in asdict(selected).items()}
    elif operation == "status":
        result = asdict(permission_asof(frame, **options))
    elif operation == "calendar":
        book = CalendarBook([PurposeCalendar(**x) for x in request["calendars"]])
        cal = book.asof(options["calendar_id"], options["purpose"], options["at"], options["on"])
        result = {
            "calendar": asdict(cal),
            "due": cal.advance(
                options["on"], options["lag"], at=options["at"], purpose=options["purpose"]
            )
            .date()
            .isoformat(),
        }
    elif operation == "macro":
        result = macro_asof(frame, **options).to_dict("records")
    elif operation == "lookthrough":
        leaves = look_through(frame, **options)
        result = {"summary": exposure_summary(leaves), "leaves": leaves.to_dict("records")}
    elif operation == "reconcile":
        result = discrepancies(frame, **options)
    elif operation == "units":
        result = normalize_trading_units(frame, **options).to_dict("records")
    elif operation == "sec-ttm":
        from quant_data_kit.us_research.sec import ttm_facts

        result = ttm_facts(frame, **options).to_dict("records")
    else:
        raise ValueError("unknown financial operation")
    return {
        "schema": SCHEMA_VERSION,
        "operation": operation,
        "request_sha256": digest(request),
        "result": result,
        "validation_scope": "supplied_facts_not_market_coverage",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    request = json.loads(Path(args.input).read_text(encoding="utf-8"))
    result = run_request(request)
    with Path(args.output).open("xb") as stream:
        stream.write(canonical(result))
    print(f"{result['operation']}: {args.output}; no market-coverage certification")


if __name__ == "__main__":
    main()
