import pandas as pd

from quant_data_kit.catalog import DataCatalog
from quant_data_kit.storage import save_parquet


def test_catalog_register_and_list(tmp_path) -> None:
    parquet = tmp_path / "prices.parquet"
    df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "symbol": ["A", "A"], "close": [1.0, 2.0]})
    save_parquet(df, parquet)

    catalog = DataCatalog(tmp_path / "catalog.yaml")
    record = catalog.register("demo", parquet)
    assert record.rows == 2
    listed = catalog.list()
    assert len(listed) == 1
    assert listed[0].dataset_id == "demo"
