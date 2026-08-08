import yaml

from quant_data_kit.catalog import DataCatalog
from quant_data_kit.cli import main_catalog


def test_catalog_registers_quant_factors_stack(tmp_path) -> None:
    catalog = DataCatalog(tmp_path / "catalog.yaml")
    deps = catalog.list_stack_dependencies()
    assert "quant-factors" in deps
    assert deps["quant-factors"]


def test_qdk_catalog_list_shows_factors_version(tmp_path) -> None:
    catalog_path = tmp_path / "catalog.yaml"
    DataCatalog(catalog_path)
    assert main_catalog(["--catalog", str(catalog_path), "list"]) == 0
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    assert payload["stack_dependencies"]["quant-factors"]
