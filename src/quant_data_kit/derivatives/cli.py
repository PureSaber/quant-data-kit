"""python -m quant_data_kit.derivatives.cli --help"""

import argparse
import json
from pathlib import Path

from .bundle import load_bundle, parse_quotes, write_bundle
from .demo import write_demo
from .models import Contract, record
from .providers import collect


def main(argv=None):
    parser = argparse.ArgumentParser(description="Immutable derivative research datasets")
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("validate")
    check.add_argument("--bundle", required=True)
    demo = sub.add_parser("demo")
    demo.add_argument("--kind", choices=["future", "option"], required=True)
    demo.add_argument("--output", required=True)
    imp = sub.add_parser("import-csv")
    imp.add_argument("--quotes", required=True, help="normalized quotes.csv")
    imp.add_argument("--limits", required=True)
    fetch = sub.add_parser("fetch")
    fetch.add_argument("--provider", choices=["dataway", "databento"], required=True)
    fetch.add_argument("--start", required=True)
    fetch.add_argument("--end", required=True)
    fetch.add_argument("--dataset")
    fetch.add_argument("--max-cost-usd")
    free = sub.add_parser("free-fetch", help="Cboe VX, NSE index F&O, Deribit coin-option analysis")
    free.add_argument("--provider", choices=["cboe-vix", "nse-fo", "deribit"], required=True)
    free.add_argument("--kind", choices=["future", "option"], required=True)
    free.add_argument(
        "--selection",
        required=True,
        help="comma-separated VX expiry dates, NSE FinInstrmNm or Deribit symbols",
    )
    free.add_argument("--start", required=True)
    free.add_argument("--end", required=True)
    free.add_argument("--source-mode", choices=["daily", "trades", "snapshot"], default="daily")
    free.add_argument(
        "--tick",
        default="0.05",
        help="explicit research tick assumption, not historical rule certification",
    )
    free.add_argument("--initial-margin")
    free.add_argument("--maintenance-margin")
    free.add_argument(
        "--files-directory",
        help="offline official files; expected exact filenames appear in documentation/receipts",
    )
    free.add_argument("--output", required=True)
    free.add_argument("--rights-note", required=True)
    for p in (imp, fetch):
        p.add_argument("--contracts", required=True, help="explicit contract rules JSON")
        p.add_argument("--output", required=True)
        p.add_argument("--rights-note", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            result = load_bundle(args.bundle)
        elif args.command == "demo":
            result = write_demo(args.output, args.kind)
        elif args.command == "free-fetch":
            from .free_sources import collect_free

            options = vars(args).copy()
            options.pop("command")
            output = options.pop("output")
            result = collect_free(output, **options)
        else:
            contracts = [
                record(Contract, c)
                for c in json.loads(Path(args.contracts).read_text(encoding="utf-8-sig"))
            ]
            if args.command == "import-csv":
                result = write_bundle(
                    args.output,
                    contracts,
                    parse_quotes(Path(args.quotes).read_bytes()),
                    provider="local-csv",
                    evidence_kind="retrospective",
                    rights_note=args.rights_note,
                    limits=args.limits,
                )
            else:
                extra = {}
                if args.provider == "databento":
                    if args.max_cost_usd is None or args.dataset is None:
                        raise ValueError("Databento requires --dataset and --max-cost-usd")
                    extra.update(dataset=args.dataset, max_cost_usd=args.max_cost_usd)
                result = collect(
                    args.output,
                    contracts,
                    args.start,
                    args.end,
                    provider=args.provider,
                    rights_note=args.rights_note,
                    **extra,
                )
        print(json.dumps(result.summary(), ensure_ascii=True, indent=2))
        return 0
    except (ValueError, OSError, KeyError, TypeError) as exc:
        parser.exit(2, f"derivative input rejected: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
