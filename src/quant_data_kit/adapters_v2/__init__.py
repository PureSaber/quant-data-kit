"""Cross-venue M2 fixture adapters."""

from quant_data_kit.adapters_v2.base import (
    AdapterContext,
    AdapterInstrument,
    BookSequenceNormalizer,
    ProviderAdapter,
    adapt_fixture_messages,
)
from quant_data_kit.adapters_v2.binance import BinanceFixtureAdapter
from quant_data_kit.adapters_v2.cn_neutral import CNNeutralFixtureAdapter
from quant_data_kit.adapters_v2.okx import OKXFixtureAdapter

__all__ = [
    "AdapterContext",
    "AdapterInstrument",
    "BinanceFixtureAdapter",
    "BookSequenceNormalizer",
    "CNNeutralFixtureAdapter",
    "OKXFixtureAdapter",
    "ProviderAdapter",
    "adapt_fixture_messages",
]
