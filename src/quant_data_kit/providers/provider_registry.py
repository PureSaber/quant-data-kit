"""Lazy registry for optional market-data providers.

The registry describes capabilities without importing optional SDKs.  Provider
modules are imported only when a caller explicitly selects one.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import asdict, dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class ProviderSpec:
    name: str
    loader: str
    capabilities: frozenset[str]
    extra: str
    credential_env: str | None = None
    max_workers: int = 4
    single_source: bool = True
    notes: str = ""

    def public_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("loader")
        payload["capabilities"] = sorted(self.capabilities)
        payload["credential_configured"] = (
            True if self.credential_env is None else bool(os.environ.get(self.credential_env))
        )
        return payload


_SPECS = {
    "akshare_auto": ProviderSpec(
        name="akshare_auto",
        loader="quant_data_kit.providers.equity_prices.akshare:fetch_auto",
        capabilities=frozenset({"prices.raw", "prices.adjusted"}),
        extra="akshare",
        max_workers=2,
        single_source=False,
        notes="Legacy Eastmoney-to-Tencent fallback; excluded from frozen research bundles.",
    ),
    "akshare_eastmoney": ProviderSpec(
        name="akshare_eastmoney",
        loader="quant_data_kit.providers.equity_prices.akshare:fetch_eastmoney",
        capabilities=frozenset({"prices.raw", "prices.adjusted"}),
        extra="akshare",
        max_workers=2,
    ),
    "akshare_tencent": ProviderSpec(
        name="akshare_tencent",
        loader="quant_data_kit.providers.equity_prices.akshare:fetch_tencent",
        capabilities=frozenset({"prices.raw", "prices.adjusted"}),
        extra="akshare",
        max_workers=2,
    ),
    "baostock": ProviderSpec(
        name="baostock",
        loader="quant_data_kit.providers.equity_prices.baostock:fetch_prices",
        capabilities=frozenset({"prices.raw", "prices.adjusted"}),
        extra="baostock",
        max_workers=1,
        notes="BaoStock sessions are serialized.",
    ),
    "tushare": ProviderSpec(
        name="tushare",
        loader="quant_data_kit.providers.equity_prices.tushare:fetch_prices",
        capabilities=frozenset({"prices.raw", "prices.adjusted"}),
        extra="tushare",
        credential_env="TUSHARE_TOKEN",
        max_workers=1,
    ),
    "yahoo": ProviderSpec(
        name="yahoo",
        loader="quant_data_kit.providers.equity_prices.yahoo:fetch_prices",
        capabilities=frozenset({"prices.raw", "prices.adjusted"}),
        extra="yahoo",
        max_workers=2,
        notes="Research-use Yahoo data accessed through yfinance.",
    ),
    "alpha_vantage": ProviderSpec(
        name="alpha_vantage",
        loader="quant_data_kit.providers.equity_prices.alphavantage:fetch_prices",
        capabilities=frozenset({"prices.raw", "prices.adjusted"}),
        extra="alpha-vantage",
        credential_env="ALPHAVANTAGE_API_KEY",
        max_workers=1,
        notes="Full and adjusted daily history may require a paid entitlement.",
    ),
}

_ALIASES = {
    "akshare": "akshare_auto",
    "eastmoney": "akshare_eastmoney",
    "tencent": "akshare_tencent",
    "yfinance": "yahoo",
    "alphavantage": "alpha_vantage",
    "alpha-vantage": "alpha_vantage",
}


def normalize_provider_name(name: str) -> str:
    normalized = str(name).strip().lower().replace("-", "_")
    return _ALIASES.get(normalized, normalized)


def get_provider_spec(name: str) -> ProviderSpec:
    normalized = normalize_provider_name(name)
    try:
        return _SPECS[normalized]
    except KeyError as exc:
        raise ValueError(
            f"Unknown price provider {name!r}; choose one of {sorted(_SPECS)}"
        ) from exc


def list_provider_specs(capability: str | None = None) -> tuple[ProviderSpec, ...]:
    specs = tuple(_SPECS[name] for name in sorted(_SPECS))
    if capability is None:
        return specs
    return tuple(spec for spec in specs if capability in spec.capabilities)


def load_price_fetcher(name: str, adjustment: str) -> tuple[ProviderSpec, Callable[..., Any]]:
    spec = get_provider_spec(name)
    capability = "prices.raw" if not adjustment else "prices.adjusted"
    if capability not in spec.capabilities:
        raise ValueError(f"Provider {spec.name} does not support {capability}")
    if spec.credential_env and not os.environ.get(spec.credential_env):
        raise RuntimeError(
            f"Provider {spec.name} requires environment variable {spec.credential_env}"
        )
    module_name, attribute = spec.loader.split(":", 1)
    module = import_module(module_name)
    return spec, getattr(module, attribute)
