"""Build immutable research inputs from one primary source per data domain."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
import yaml

from quant_data_kit.calendar import load_sse_trade_dates
from quant_data_kit.providers._symbols import normalize_symbol
from quant_data_kit.providers.benchmark import fetch_hs300_benchmark
from quant_data_kit.providers.corporate_actions import fetch_corporate_actions
from quant_data_kit.providers.prices import fetch_daily_prices
from quant_data_kit.providers.provider_registry import get_provider_spec, normalize_provider_name
from quant_data_kit.providers.tradability import fetch_tradability
from quant_data_kit.validate import validate_price_frame

SCHEMA_VERSION = "qdk.research-inputs/v1"
_ROOT_KEYS = {
    "schema_version",
    "prices",
    "benchmark",
    "calendar",
    "corporate_actions",
    "trading_status",
}


def _canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping(value: object, name: str) -> dict:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    return value


def load_policy(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    policy = _mapping(payload, "research input policy")
    unknown = set(policy) - _ROOT_KEYS
    if unknown:
        raise ValueError(f"Unknown research input policy fields: {sorted(unknown)}")
    if policy.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}")

    prices = _mapping(policy.get("prices"), "prices")
    unknown_prices = set(prices) - {
        "primary",
        "shadows",
        "max_workers",
        "max_retries",
        "sleep_seconds",
    }
    if unknown_prices:
        raise ValueError(f"Unknown prices fields: {sorted(unknown_prices)}")
    primary = normalize_provider_name(prices.get("primary", ""))
    spec = get_provider_spec(primary)
    if not spec.single_source:
        raise ValueError("Frozen research inputs require one provider; fallback providers are forbidden")
    required = {"prices.raw", "prices.adjusted"}
    if not required.issubset(spec.capabilities):
        raise ValueError(f"Primary provider {primary} lacks raw or adjusted daily prices")
    shadows = [normalize_provider_name(item) for item in prices.get("shadows", [])]
    if len(shadows) != len(set(shadows)) or primary in shadows:
        raise ValueError("Price shadows must be unique and different from the primary")
    for shadow in shadows:
        shadow_spec = get_provider_spec(shadow)
        if not shadow_spec.single_source or "prices.raw" not in shadow_spec.capabilities:
            raise ValueError(f"Invalid price shadow provider: {shadow}")

    normalized = {
        "schema_version": SCHEMA_VERSION,
        "prices": {
            "primary": primary,
            "shadows": shadows,
            "max_workers": int(prices.get("max_workers", 1)),
            "max_retries": int(prices.get("max_retries", 2)),
            "sleep_seconds": float(prices.get("sleep_seconds", 0.2)),
        },
        "benchmark": {
            "provider": _mapping(policy.get("benchmark", {}), "benchmark").get(
                "provider", "akshare_eastmoney"
            )
        },
        "calendar": {
            "provider": _mapping(policy.get("calendar", {}), "calendar").get(
                "provider", "akshare_sina"
            )
        },
        "corporate_actions": {
            "provider": _mapping(
                policy.get("corporate_actions", {}), "corporate_actions"
            ).get("provider", "cninfo"),
            "required": bool(
                _mapping(policy.get("corporate_actions", {}), "corporate_actions").get(
                    "required", True
                )
            ),
        },
        "trading_status": {
            "provider": _mapping(policy.get("trading_status", {}), "trading_status").get(
                "provider", "akshare_eastmoney"
            ),
            "required": bool(
                _mapping(policy.get("trading_status", {}), "trading_status").get(
                    "required", False
                )
            ),
        },
    }
    if normalized["benchmark"]["provider"] not in {"akshare_eastmoney", "akshare_sina"}:
        raise ValueError("Unsupported HS300 benchmark provider")
    if normalized["calendar"]["provider"] != "akshare_sina":
        raise ValueError("Unsupported SSE calendar provider")
    if normalized["corporate_actions"]["provider"] != "cninfo":
        raise ValueError("Unsupported corporate-action provider")
    if normalized["trading_status"]["provider"] != "akshare_eastmoney":
        raise ValueError("Unsupported trading-status provider")
    price_options = normalized["prices"]
    if (
        price_options["max_workers"] < 1
        or price_options["max_retries"] < 1
        or price_options["sleep_seconds"] < 0
    ):
        raise ValueError("Price worker, retry and sleep settings are invalid")
    return normalized


def _write_frame(root: Path, name: str, frame: pd.DataFrame, provider: str) -> dict:
    path = root / f"{name}.parquet"
    frame.to_parquet(path, index=False)
    return {
        "file": path.name,
        "sha256": _sha256(path),
        "rows": len(frame),
        "provider": provider,
    }


def _compare_prices(primary: pd.DataFrame, shadow: pd.DataFrame) -> dict:
    keys = ["symbol", "date"]
    left = primary[keys + ["close", "volume"]].copy()
    right = shadow[keys + ["close", "volume"]].copy()
    merged = left.merge(right, on=keys, how="outer", suffixes=("_primary", "_shadow"), indicator=True)
    common = merged[merged["_merge"] == "both"].copy()
    if common.empty:
        return {
            "primary_rows": len(left),
            "shadow_rows": len(right),
            "common_rows": 0,
            "missing_in_shadow": int((merged["_merge"] == "left_only").sum()),
            "missing_in_primary": int((merged["_merge"] == "right_only").sum()),
            "max_close_relative_difference": None,
            "median_close_relative_difference": None,
        }
    denominator = common["close_primary"].abs().replace(0, float("nan"))
    differences = ((common["close_shadow"] - common["close_primary"]).abs() / denominator).dropna()
    return {
        "primary_rows": len(left),
        "shadow_rows": len(right),
        "common_rows": len(common),
        "missing_in_shadow": int((merged["_merge"] == "left_only").sum()),
        "missing_in_primary": int((merged["_merge"] == "right_only").sum()),
        "max_close_relative_difference": float(differences.max()) if len(differences) else None,
        "median_close_relative_difference": (
            float(differences.median()) if len(differences) else None
        ),
    }


def _price_kwargs(policy: dict, provider: str) -> dict:
    options = policy["prices"]
    spec = get_provider_spec(provider)
    return {
        "provider": provider,
        "max_workers": min(options["max_workers"], spec.max_workers),
        "max_retries": options["max_retries"],
        "sleep_seconds": options["sleep_seconds"],
    }


def build_research_inputs(
    policy: dict,
    output: Path,
    *,
    symbols: list[str],
    start: str,
    end: str,
    captured_at: str,
) -> dict:
    """Fetch, validate and atomically publish one immutable input directory."""
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"Research input directory already exists: {output}")
    captured = pd.Timestamp(captured_at)
    if captured.tzinfo is None:
        raise ValueError("captured_at must include a timezone")
    start_day = pd.Timestamp(start).normalize()
    end_day = pd.Timestamp(end).normalize()
    if start_day > end_day:
        raise ValueError("start must not be after end")
    captured_day = captured.tz_convert("Asia/Shanghai").tz_localize(None).normalize()
    if end_day > captured_day:
        raise ValueError("Research inputs cannot request a future session")
    normalized_symbols = list(dict.fromkeys(normalize_symbol(symbol) for symbol in symbols))
    if not normalized_symbols:
        raise ValueError("At least one symbol is required")

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.parent / f".{output.name}.tmp-{uuid4().hex}"
    temporary.mkdir()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "origin": "live_public_api",
        "captured_at": captured.isoformat(),
        "requested_start": start_day.date().isoformat(),
        "requested_end": end_day.date().isoformat(),
        "symbols": normalized_symbols,
        "policy": policy,
        "policy_sha256": hashlib.sha256(_canonical_bytes(policy)).hexdigest(),
        "files": {},
        "shadows": {},
        "warnings": [],
    }
    try:
        primary = policy["prices"]["primary"]
        raw = fetch_daily_prices(
            normalized_symbols,
            str(start_day.date()),
            str(end_day.date()),
            adjust="",
            **_price_kwargs(policy, primary),
        )
        adjusted = fetch_daily_prices(
            normalized_symbols,
            str(start_day.date()),
            str(end_day.date()),
            adjust="qfq",
            **_price_kwargs(policy, primary),
        )
        validate_price_frame(raw)
        validate_price_frame(adjusted)
        if set(map(tuple, raw[["symbol", "date"]].to_numpy())) != set(
            map(tuple, adjusted[["symbol", "date"]].to_numpy())
        ):
            raise ValueError("Primary raw and adjusted price keys differ")
        manifest["files"]["raw"] = _write_frame(temporary, "raw", raw, primary)
        manifest["files"]["adjusted"] = _write_frame(
            temporary, "adjusted", adjusted, primary
        )

        benchmark_provider = policy["benchmark"]["provider"]
        benchmark = fetch_hs300_benchmark(
            str(start_day.date()), str(end_day.date()), provider=benchmark_provider
        )
        if benchmark.empty:
            raise ValueError("HS300 benchmark response is empty")
        manifest["files"]["benchmark"] = _write_frame(
            temporary, "benchmark", benchmark, benchmark_provider
        )

        calendar_provider = policy["calendar"]["provider"]
        calendar = pd.DataFrame({"date": load_sse_trade_dates(provider=calendar_provider)})
        if calendar.empty:
            raise ValueError("SSE trading calendar response is empty")
        manifest["files"]["calendar"] = _write_frame(
            temporary, "calendar", calendar, calendar_provider
        )

        actions_policy = policy["corporate_actions"]
        try:
            actions = fetch_corporate_actions(normalized_symbols, captured.isoformat())
            manifest["files"]["actions"] = _write_frame(
                temporary, "actions", actions, actions_policy["provider"]
            )
        except Exception as exc:
            if actions_policy["required"]:
                raise RuntimeError("Required corporate-action source failed") from exc
            manifest["warnings"].append(
                {"domain": "corporate_actions", "error_type": type(exc).__name__}
            )

        status_policy = policy["trading_status"]
        try:
            status = fetch_tradability(
                normalized_symbols, end_day.date().isoformat(), captured.isoformat()
            )
            manifest["files"]["status"] = _write_frame(
                temporary, "status", status, status_policy["provider"]
            )
        except Exception as exc:
            if status_policy["required"]:
                raise RuntimeError("Required trading-status source failed") from exc
            manifest["warnings"].append(
                {"domain": "trading_status", "error_type": type(exc).__name__}
            )

        shadow_root = temporary / "shadows"
        for shadow_provider in policy["prices"]["shadows"]:
            try:
                shadow = fetch_daily_prices(
                    normalized_symbols,
                    str(start_day.date()),
                    str(end_day.date()),
                    adjust="",
                    **_price_kwargs(policy, shadow_provider),
                )
                validate_price_frame(shadow)
                shadow_root.mkdir(exist_ok=True)
                path = shadow_root / f"{shadow_provider}-raw.parquet"
                shadow.to_parquet(path, index=False)
                manifest["shadows"][shadow_provider] = {
                    "status": "available",
                    "file": str(path.relative_to(temporary)).replace("\\", "/"),
                    "sha256": _sha256(path),
                    "comparison": _compare_prices(raw, shadow),
                }
            except Exception as exc:  # noqa: BLE001 - shadows never replace the primary
                manifest["shadows"][shadow_provider] = {
                    "status": "unavailable",
                    "error_type": type(exc).__name__,
                }
                manifest["warnings"].append(
                    {"domain": "price_shadow", "provider": shadow_provider, "error_type": type(exc).__name__}
                )

        (temporary / "manifest.json").write_bytes(_canonical_bytes(manifest))
        temporary.replace(output)
        return manifest
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="qdk-research-inputs",
        description="Build an immutable normalized research-input directory",
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--captured-at")
    args = parser.parse_args(argv)
    captured_at = args.captured_at or datetime.now(timezone.utc).isoformat()
    policy = load_policy(args.config.resolve())
    manifest = build_research_inputs(
        policy,
        args.output,
        symbols=args.symbols,
        start=args.start,
        end=args.end,
        captured_at=captured_at,
    )
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "policy_sha256": manifest["policy_sha256"],
                "providers": {
                    name: entry["provider"] for name, entry in manifest["files"].items()
                },
                "warnings": manifest["warnings"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
