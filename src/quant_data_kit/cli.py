"""CLI for quant-data-kit."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from quant_data_kit.storage import load_manifest, load_parquet
from quant_data_kit.validate import validate_price_frame


def main_validate(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qdk-validate", description="Validate a Parquet price dataset")
    parser.add_argument("parquet", type=Path, help="Path to Parquet file")
    args = parser.parse_args(argv)
    df = load_parquet(args.parquet)
    stats = validate_price_frame(df)
    print(json.dumps(stats, indent=2))
    return 0


def main_manifest(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qdk-manifest", description="Print dataset manifest JSON")
    parser.add_argument("manifest", type=Path, help="Path to *.manifest.json")
    args = parser.parse_args(argv)
    manifest = load_manifest(args.manifest)
    print(json.dumps(manifest.__dict__, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main_validate(sys.argv[1:]))
