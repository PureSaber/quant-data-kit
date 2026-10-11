"""JSON/JSONL command-line interface to the local artifact and snapshot stores."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_data_kit.artifacts import ArtifactRef, ArtifactStore, safe_path
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.shared_snapshots import SnapshotStore


def _ref(path: Path) -> ArtifactRef:
    payload = json.loads(safe_path(path, regular=True).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValidationError("Reference JSON must be an object")
    return ArtifactRef.from_dict(payload.get("artifact", payload))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--run-id", default="cli")
    commands = parser.add_subparsers(dest="command", required=True)
    put = commands.add_parser("put")
    put.add_argument("path", type=Path)
    put.add_argument("--owner", required=True)
    put.add_argument("--media-type")
    publish = commands.add_parser("publish")
    publish.add_argument("--member", action="append", required=True, help="NAME=PARQUET_PATH")
    publish.add_argument("--dataset", required=True)
    publish.add_argument("--version", required=True)
    publish.add_argument("--temporal-json", required=True)
    publish.add_argument("--owner", required=True)
    publish.add_argument("--date-column", default="date")
    publish.add_argument("--symbol-column", default="symbol")
    publish.add_argument("--available-at-column")
    legacy = commands.add_parser("import-snapshot")
    legacy.add_argument("manifest", type=Path)
    legacy.add_argument("--owner", required=True)
    for name in ("verify", "inspect", "query", "retain"):
        command = commands.add_parser(name)
        command.add_argument(
            "--ref", required=True, type=Path, help="ArtifactRef or snapshot JSON file"
        )
        if name == "retain":
            command.add_argument("--owner", required=True)
        if name == "query":
            command.add_argument("--columns", nargs="+")
            command.add_argument("--start-date")
            command.add_argument("--end-date")
            command.add_argument("--symbols", nargs="+")
            command.add_argument("--as-of")
            command.add_argument("--batch-size", type=int, default=65536)
    release = commands.add_parser("release")
    release.add_argument("--owner", required=True)
    gc = commands.add_parser("gc")
    gc.add_argument("--min-age-seconds", type=float, default=3600)
    args = parser.parse_args(argv)
    try:
        artifacts = ArtifactStore(args.root, context={"run_id": args.run_id})
        snapshots = SnapshotStore(artifacts)
        if args.command == "put":
            result = artifacts.put_file(
                args.path, owner=args.owner, media_type=args.media_type
            ).to_dict()
        elif args.command == "publish":
            files = {}
            for member in args.member:
                name, separator, path = member.partition("=")
                if not separator or not path or name in files:
                    raise ValidationError("Members must have unique NAME=PARQUET_PATH values")
                files[name] = Path(path)
            result = snapshots.publish(
                files,
                dataset=args.dataset,
                version=args.version,
                temporal=json.loads(args.temporal_json),
                owner=args.owner,
                date_column=args.date_column,
                symbol_column=args.symbol_column,
                available_at_column=args.available_at_column,
            ).to_dict()
        elif args.command == "import-snapshot":
            result = snapshots.import_snapshot(args.manifest, owner=args.owner).to_dict()
        elif args.command == "verify":
            artifacts.verify(_ref(args.ref))
            result = {"verified": True}
        elif args.command == "inspect":
            result = snapshots.load(_ref(args.ref)).to_dict()
        elif args.command == "query":
            for batch in snapshots.iter_batches(
                _ref(args.ref),
                columns=args.columns,
                start_date=args.start_date,
                end_date=args.end_date,
                symbols=args.symbols,
                as_of=args.as_of,
                batch_size=args.batch_size,
            ):
                for row in batch.to_pylist():
                    print(json.dumps(row, ensure_ascii=False, default=str, allow_nan=False))
            return 0
        elif args.command == "retain":
            artifacts.retain(args.owner, [_ref(args.ref)])
            result = {"retained": True}
        elif args.command == "release":
            result = {"released": artifacts.release(args.owner)}
        else:
            result = {"removed": artifacts.collect(min_age_seconds=args.min_age_seconds)}
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except (ValidationError, OSError, ValueError, TypeError) as exc:
        parser.exit(2, f"qdk-artifacts: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
