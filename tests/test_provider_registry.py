import pytest

from quant_data_kit.providers.provider_registry import (
    get_provider_spec,
    list_provider_specs,
    normalize_provider_name,
)


def test_registry_lists_optional_providers_without_importing_sdks():
    names = {spec.name for spec in list_provider_specs("prices.raw")}
    assert {
        "akshare_eastmoney",
        "akshare_tencent",
        "baostock",
        "tushare",
        "yahoo",
        "alpha_vantage",
    }.issubset(names)
    assert get_provider_spec("yfinance").name == "yahoo"
    assert normalize_provider_name("alpha-vantage") == "alpha_vantage"
    assert get_provider_spec("akshare_auto").single_source is False
    public = get_provider_spec("akshare_eastmoney").public_dict()
    assert public["dependency_module"] == "akshare"
    assert isinstance(public["installed"], bool)
    with pytest.raises(ValueError, match="Unknown price provider"):
        get_provider_spec("missing")
