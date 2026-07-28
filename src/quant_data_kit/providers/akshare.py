"""AKShare data provider helpers (optional dependency)."""

from __future__ import annotations

from quant_data_kit.providers._network import configure_network
from quant_data_kit.providers._symbols import normalize_symbol, to_market_symbol
from quant_data_kit.providers.prices import PRICE_COLUMNS, fetch_daily_prices
from quant_data_kit.providers.universe import fetch_hs300_constituents

__all__ = [
    "PRICE_COLUMNS",
    "configure_network",
    "fetch_daily_prices",
    "fetch_hs300_constituents",
    "normalize_symbol",
    "to_market_symbol",
]
