"""Safe-by-default CLI for the public Crypto L2 collector."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from quant_data_kit.capture_v2.collector import CryptoL2CaptureCoordinator
from quant_data_kit.capture_v2.models import (
    CaptureConfig,
    MarketKind,
    Provider,
    RetryPolicy,
    SegmentRotation,
    StreamConfig,
    default_crypto_l2_streams,
)
from quant_data_kit.exceptions import ValidationError

_CONFIG_KEYS = {
    "hot_root",
    "archive_root",
    "restore_root",
    "collector_commit",
    "streams",
    "rotation",
    "retry",
    "archive_reserve_bytes",
}
_STREAM_KEYS = {
    "stream_id",
    "provider",
    "market",
    "native_symbol",
    "instrument_id",
    "venue",
    "websocket_url",
    "channel",
    "price_scale",
    "quantity_scale",
    "rest_snapshot_url",
}
_FORBIDDEN_KEY_PARTS = ("api_key", "secret", "token", "account", "authorization", "headers")


def _reject_credentials(value: Any, path: str = "config") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(part in normalized for part in _FORBIDDEN_KEY_PARTS):
                raise ValidationError(
                    f"public market-data capture rejects credential/account field: {path}.{key}"
                )
            _reject_credentials(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_credentials(item, f"{path}[{index}]")


def load_capture_config(path: Path) -> CaptureConfig:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError(f"capture config is unreadable or malformed: {path}") from exc
    if not isinstance(payload, dict):
        raise ValidationError("capture config must be a JSON object")
    _reject_credentials(payload)
    unknown = sorted(set(payload).difference(_CONFIG_KEYS))
    if unknown:
        raise ValidationError(f"capture config contains unsupported fields: {unknown}")
    required = {"hot_root", "archive_root", "restore_root", "collector_commit"}
    missing = sorted(required.difference(payload))
    if missing:
        raise ValidationError(f"capture config is missing explicit fields: {missing}")
    streams = _streams(payload.get("streams"))
    rotation_payload = payload.get("rotation", {})
    retry_payload = payload.get("retry", {})
    if not isinstance(rotation_payload, dict) or not isinstance(retry_payload, dict):
        raise ValidationError("rotation and retry must be JSON objects")
    try:
        return CaptureConfig(
            hot_root=Path(str(payload["hot_root"])),
            archive_root=Path(str(payload["archive_root"])),
            restore_root=Path(str(payload["restore_root"])),
            collector_commit=str(payload["collector_commit"]),
            streams=streams,
            rotation=SegmentRotation(**rotation_payload),
            retry=RetryPolicy(**retry_payload),
            archive_reserve_bytes=int(payload.get("archive_reserve_bytes", 150 * 1024**3)),
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"capture config values are invalid: {exc}") from exc


def _streams(value: Any) -> tuple[StreamConfig, ...]:
    if value is None:
        return default_crypto_l2_streams()
    if not isinstance(value, list) or not value:
        raise ValidationError("streams must be a non-empty JSON array")
    streams: list[StreamConfig] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValidationError(f"streams[{index}] must be an object")
        unknown = sorted(set(item).difference(_STREAM_KEYS))
        if unknown:
            raise ValidationError(f"streams[{index}] contains unsupported fields: {unknown}")
        try:
            values = dict(item)
            values["provider"] = Provider(values["provider"])
            values["market"] = MarketKind(values["market"])
            streams.append(StreamConfig(**values))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError(f"streams[{index}] is invalid: {exc}") from exc
    return tuple(streams)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qdk-capture",
        description="Fail-closed public Binance/OKX Crypto L2 capture",
    )
    parser.add_argument("config", type=Path, help="Explicit JSON config; no credentials accepted")
    parser.add_argument(
        "--mode",
        choices=("preflight", "probe", "run"),
        default="preflight",
        help="Default only validates archive/capacity and opens no network connection",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=3,
        help="Per-stream public WebSocket message bound in probe mode",
    )
    parser.add_argument(
        "--confirm-long-running",
        action="store_true",
        help="Required for run mode after archive preflight succeeds",
    )
    return parser


def main_capture(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_capture_config(args.config)
        coordinator = CryptoL2CaptureCoordinator(config)
        if args.mode == "preflight":
            report = coordinator.preflight_only()
        elif args.mode == "probe":
            if args.max_messages < 2:
                raise ValidationError("probe mode requires --max-messages>=2")
            report = asyncio.run(coordinator.run(maximum_websocket_messages=args.max_messages))
        else:
            if not args.confirm_long_running:
                raise ValidationError("run mode requires explicit --confirm-long-running")
            report = asyncio.run(coordinator.run(maximum_websocket_messages=None))
    except (ValidationError, OSError) as exc:
        print(json.dumps({"status": "PAUSED", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(asdict(report), indent=2, ensure_ascii=False))
    return 0 if "FAILED" not in report.status else 2


if __name__ == "__main__":
    raise SystemExit(main_capture())
