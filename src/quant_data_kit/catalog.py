from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

import yaml

from quant_data_kit.storage import load_parquet


@dataclass
class DatasetRecord:
    dataset_id: str
    path: str
    manifest_path: str
    rows: int
    columns: list[str]
    date_min: str
    date_max: str
    registered_at: str


class DataCatalog:
    DEFAULT_STACK: ClassVar[dict[str, str]] = {"quant-factors": "0.1.0"}

    def __init__(self, catalog_path: Path) -> None:
        self.catalog_path = Path(catalog_path)
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        if self.catalog_path.is_file():
            raw = yaml.safe_load(self.catalog_path.read_text(encoding="utf-8")) or {}
            self._records: dict[str, dict] = raw.get("datasets") or {}
            self._stack: dict[str, str] = raw.get("stack_dependencies") or {}
        else:
            self._records = {}
            self._stack = {}
        self.ensure_default_stack()

    def ensure_default_stack(self) -> None:
        changed = False
        for name, version in self.DEFAULT_STACK.items():
            if name not in self._stack:
                self._stack[name] = version
                changed = True
        if changed or not self.catalog_path.is_file():
            self.save()

    def list_stack_dependencies(self) -> dict[str, str]:
        return dict(self._stack)

    def register_stack_dependency(self, name: str, version: str) -> None:
        self._stack[name] = version
        self.save()

    def save(self) -> None:
        payload = {"datasets": self._records, "stack_dependencies": self._stack}
        self.catalog_path.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8"
        )

    def register(
        self, dataset_id: str, parquet_path: Path, manifest_path: Path | None = None
    ) -> DatasetRecord:
        parquet_path = Path(parquet_path)
        df = load_parquet(parquet_path)
        manifest_path = manifest_path or parquet_path.with_suffix(".manifest.json")

        date_min = date_max = ""
        if "date" in df.columns and not df.empty:
            dates = df["date"].astype(str)
            date_min, date_max = str(dates.min()), str(dates.max())

        record = DatasetRecord(
            dataset_id=dataset_id,
            path=str(parquet_path.resolve()),
            manifest_path=str(manifest_path.resolve()) if manifest_path.is_file() else "",
            rows=len(df),
            columns=list(df.columns),
            date_min=date_min,
            date_max=date_max,
            registered_at=datetime.now(timezone.utc).isoformat(),
        )
        self._records[dataset_id] = asdict(record)
        self.save()
        return record

    def list(self) -> list[DatasetRecord]:
        return [DatasetRecord(**payload) for payload in self._records.values()]

    def get(self, dataset_id: str) -> DatasetRecord | None:
        payload = self._records.get(dataset_id)
        return DatasetRecord(**payload) if payload else None
