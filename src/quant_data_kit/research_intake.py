"""Immutable research-data intake, scoped admission checks, and unified reads.

This module is intentionally executable by file path.  A configured QDK Python
interpreter can run it without installing the current checkout first::

    python src/quant_data_kit/research_intake.py inspect-source --source data.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

# When this file is executed directly, its package directory would otherwise
# shadow the standard-library ``calendar`` module with ``quant_data_kit/calendar.py``.
# Remove that entry before importing pandas, then expose the checkout's src root.
if __package__ in {None, ""}:
    _SCRIPT_DIRECTORY = Path(__file__).resolve().parent
    sys.path = [
        entry
        for entry in sys.path
        if not entry or Path(entry).resolve() != _SCRIPT_DIRECTORY
    ]
    sys.path.insert(0, str(_SCRIPT_DIRECTORY.parent))

import argparse
import codecs
import hashlib
import json
import math
import os
import re
import shutil
import stat
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

CONTRACT_SCHEMA_VERSION = "qdk.research-intake-contract/v1"
MANIFEST_SCHEMA_VERSION = "qdk.research-intake-version/v1"
CATALOG_SCHEMA_VERSION = "qdk.research-intake-catalog/v1"
RECEIPT_SCHEMA_VERSION = "qdk.research-intake-receipt/v1"
RESPONSE_SCHEMA_VERSION = "qdk.research-intake-response/v1"
REPORT_SCHEMA_VERSION = "qdk.research-intake-report/v1"
MAX_ISSUE_SAMPLES = 20
DEFAULT_MAX_BYTES = 1024**3
DEFAULT_MAX_ROWS = 5_000_000
SUPPORTED_FORMATS = {"csv", "tsv", "parquet"}
SUPPORTED_TYPES = {"string", "integer", "number", "boolean", "date", "datetime"}
SUPPORTED_PURPOSES = {
    "exploration",
    "daily_bars_research",
    "historical_financial_factor_backtest",
}
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_WINDOWS_RESERVED = {
    "AUX",
    "CON",
    "NUL",
    "PRN",
    *(f"COM{number}" for number in range(1, 10)),
    *(f"LPT{number}" for number in range(1, 10)),
}
_INTERNAL_ROW = "_qdk_source_row"
_ISSUE_COLUMNS = [
    "row",
    "rule",
    "column",
    "value",
    "severity",
    "message",
    "recommendation",
]


class ResearchIntakeError(ValueError):
    """Base error for the research intake boundary."""


class ContractError(ResearchIntakeError):
    """Raised when an explicit import contract is invalid."""


class IntegrityError(ResearchIntakeError):
    """Raised when immutable snapshot evidence no longer matches its hashes."""


class AdmissionError(ResearchIntakeError):
    """Raised when a requested purpose and scope do not pass admission."""


@dataclass(frozen=True)
class IntakeReadResult:
    frame: pd.DataFrame
    metadata: dict[str, Any]
    check: dict[str, Any]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _jsonable(value: Any) -> Any:
    if value is pd.NA or value is pd.NaT or value is None:
        return None
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _jsonable(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    return value


def _canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        _jsonable(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = getattr(os.lstat(path), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _root_path(root: str | Path, *, create: bool) -> Path:
    path = Path(root).absolute()
    if create:
        path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise ResearchIntakeError(f"intake root is not a directory: {path}")
    if _is_reparse_point(path):
        raise ResearchIntakeError(f"intake root cannot be a symlink or reparse point: {path}")
    return path.resolve(strict=True)


def _child(root: Path, relative: str | Path, *, must_exist: bool = False) -> Path:
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise IntegrityError(f"path escapes intake root: {relative}")
    candidate = root.joinpath(relative_path)
    current = root
    for part in relative_path.parts:
        current = current / part
        if current.exists() and _is_reparse_point(current):
            raise IntegrityError(f"intake path contains a symlink or reparse point: {current}")
    if must_exist and not candidate.exists():
        raise IntegrityError(f"intake evidence is missing: {candidate}")
    try:
        candidate.resolve(strict=must_exist).relative_to(root)
    except ValueError as exc:
        raise IntegrityError(f"resolved path escapes intake root: {candidate}") from exc
    return candidate


def _safe_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_NAME.fullmatch(value):
        raise ResearchIntakeError(f"{field} must be a safe name")
    if value.rstrip(". ") != value or value.split(".", 1)[0].upper() in _WINDOWS_RESERVED:
        raise ResearchIntakeError(f"{field} is reserved: {value!r}")
    return value


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    body = _canonical_bytes(payload)
    try:
        with temporary.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"invalid JSON evidence: {path}") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"JSON evidence must be an object: {path}")
    return payload


def _catalog(root: Path) -> dict[str, Any]:
    path = _child(root, "catalog.json")
    if not path.exists():
        return {"schema_version": CATALOG_SCHEMA_VERSION, "datasets": {}}
    payload = _read_json(path)
    if payload.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise IntegrityError("unsupported research-intake catalog schema")
    if not isinstance(payload.get("datasets"), dict):
        raise IntegrityError("research-intake catalog datasets must be an object")
    return payload


def _load_contract(contract: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(contract, Mapping):
        payload = json.loads(json.dumps(_jsonable(contract), ensure_ascii=False))
    else:
        try:
            payload = json.loads(Path(contract).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ContractError(f"cannot read contract JSON: {contract}") from exc
    if not isinstance(payload, dict):
        raise ContractError("contract must be a JSON object")
    return _validate_contract(payload)


def _validate_contract(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "schema_version",
        "kind",
        "input",
        "mapping",
        "types",
        "formats",
        "primary_key",
        "metadata",
        "limits",
    }
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ContractError(f"contract contains unknown fields: {unknown}")
    if payload.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        raise ContractError(f"contract schema_version must be {CONTRACT_SCHEMA_VERSION}")
    kind = payload.get("kind")
    if kind not in {"table", "daily_bars", "history"}:
        raise ContractError("contract kind must be table, daily_bars, or history")
    input_spec = payload.get("input")
    if not isinstance(input_spec, dict):
        raise ContractError("contract input must be an object")
    file_format = input_spec.get("format")
    if file_format not in SUPPORTED_FORMATS:
        raise ContractError("contract input.format must be csv, tsv, or parquet")
    if file_format in {"csv", "tsv"}:
        encoding = input_spec.get("encoding")
        if not isinstance(encoding, str) or not encoding:
            raise ContractError("CSV/TSV contract input.encoding must be explicit")
        try:
            codecs.lookup(encoding)
        except LookupError as exc:
            raise ContractError(f"unknown input encoding: {encoding}") from exc
        delimiter = input_spec.get("delimiter")
        if delimiter == "\\t":
            delimiter = "\t"
            input_spec["delimiter"] = delimiter
        if not isinstance(delimiter, str) or len(delimiter) != 1 or delimiter in "\r\n":
            raise ContractError("CSV/TSV input.delimiter must be one explicit character")
    elif set(input_spec) - {"format"}:
        raise ContractError("Parquet input accepts only the format field")

    mapping = payload.get("mapping")
    types = payload.get("types")
    if not isinstance(mapping, dict) or not mapping:
        raise ContractError("contract mapping must be a nonempty canonical-to-source object")
    if not isinstance(types, dict) or set(types) != set(mapping):
        raise ContractError("contract types must declare every and only mapped canonical column")
    if _INTERNAL_ROW in mapping:
        raise ContractError(f"{_INTERNAL_ROW} is reserved")
    if any(not isinstance(key, str) or not key or "," in key for key in mapping):
        raise ContractError("canonical mapping names must be nonempty strings without commas")
    source_columns = list(mapping.values())
    if any(not isinstance(value, str) or not value for value in source_columns):
        raise ContractError("source mapping names must be nonempty strings")
    if len(source_columns) != len(set(source_columns)):
        raise ContractError("one source column cannot map to multiple canonical columns")
    bad_types = {name: value for name, value in types.items() if value not in SUPPORTED_TYPES}
    if bad_types:
        raise ContractError(f"unsupported canonical types: {bad_types}")
    formats = payload.setdefault("formats", {})
    if not isinstance(formats, dict) or not set(formats).issubset(mapping):
        raise ContractError("contract formats must reference mapped canonical columns")
    if any(not isinstance(value, str) or not value for value in formats.values()):
        raise ContractError("date and datetime formats must be nonempty strings")
    primary_key = payload.setdefault("primary_key", [])
    if (
        not isinstance(primary_key, list)
        or len(primary_key) != len(set(primary_key))
        or not set(primary_key).issubset(mapping)
    ):
        raise ContractError("primary_key must contain unique mapped canonical columns")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict) or not isinstance(metadata.get("source"), str):
        raise ContractError("metadata.source must be an explicit nonempty string")
    if not metadata["source"].strip():
        raise ContractError("metadata.source must be an explicit nonempty string")
    units = metadata.setdefault("units", {})
    if not isinstance(units, dict):
        raise ContractError("metadata.units must be a canonical-column-to-unit object")
    if not set(units).issubset(mapping):
        raise ContractError("metadata.units references an unmapped canonical column")
    limits = payload.setdefault("limits", {})
    if not isinstance(limits, dict) or set(limits) - {"max_bytes", "max_rows"}:
        raise ContractError("limits accepts only max_bytes and max_rows")
    for field in ("max_bytes", "max_rows"):
        if field in limits and (not isinstance(limits[field], int) or limits[field] <= 0):
            raise ContractError(f"limits.{field} must be a positive integer")
    if kind == "daily_bars":
        required = {"symbol", "date", "open", "high", "low", "close"}
        missing = sorted(required - set(mapping))
        if missing:
            raise ContractError(f"daily_bars mapping lacks required columns: {missing}")
    if kind == "history":
        required = {"symbol", "period_end"}
        missing = sorted(required - set(mapping))
        if missing:
            raise ContractError(f"history mapping lacks required columns: {missing}")
        values = set(mapping) - set(primary_key) - {"symbol", "period_end", "available_at"}
        if not values:
            raise ContractError("history requires at least one mapped value column")
    return payload


def _resolve_format(source: Path, file_format: str | None) -> str:
    if file_format is not None:
        normalized = file_format.lower().lstrip(".")
    else:
        normalized = source.suffix.lower().lstrip(".")
    if normalized not in SUPPORTED_FORMATS:
        raise ResearchIntakeError("source format must be explicitly csv, tsv, or parquet")
    return normalized


def inspect_source(
    source: str | Path,
    *,
    file_format: str | None = None,
    encoding: str = "utf-8",
    delimiter: str | None = None,
    max_sample_rows: int = 20,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    """Inspect at most twenty rows without claiming data-quality admission."""
    lexical_source = Path(source).absolute()
    if _is_reparse_point(lexical_source):
        raise ResearchIntakeError(f"source cannot be a symlink or reparse point: {lexical_source}")
    path = lexical_source.resolve(strict=True)
    if not path.is_file():
        raise ResearchIntakeError(f"source is not a regular file: {path}")
    if not 1 <= max_sample_rows <= 20:
        raise ResearchIntakeError("max_sample_rows must be between 1 and 20")
    file_bytes = path.stat().st_size
    if max_bytes <= 0 or file_bytes > max_bytes:
        raise ResearchIntakeError(
            f"source bytes {file_bytes} exceed configured max_bytes {max_bytes}"
        )
    selected_format = _resolve_format(path, file_format)
    if selected_format in {"csv", "tsv"}:
        try:
            codecs.lookup(encoding)
        except LookupError as exc:
            raise ResearchIntakeError(f"unknown source encoding: {encoding}") from exc
        separator = delimiter if delimiter is not None else ("\t" if selected_format == "tsv" else ",")
        if separator == "\\t":
            separator = "\t"
        if len(separator) != 1 or separator in "\r\n":
            raise ResearchIntakeError("delimiter must be one explicit character")
        frame = pd.read_csv(
            path,
            sep=separator,
            encoding=encoding,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            nrows=max_sample_rows,
        )
        columns = [{"name": str(name), "type": "string"} for name in frame.columns]
        output_encoding: str | None = encoding
        output_delimiter: str | None = separator
    else:
        parquet = pq.ParquetFile(path)
        batches = parquet.iter_batches(batch_size=max_sample_rows)
        first = next(batches, None)
        frame = first.to_pandas().head(max_sample_rows) if first is not None else pd.DataFrame(
            columns=parquet.schema_arrow.names
        )
        columns = [
            {"name": field.name, "type": str(field.type)} for field in parquet.schema_arrow
        ]
        output_encoding = None
        output_delimiter = None
    rows = [_jsonable(record) for record in frame.to_dict(orient="records")]
    return {
        "source": str(path),
        "format": selected_format,
        "encoding": output_encoding,
        "delimiter": output_delimiter,
        "columns": columns,
        "rows": rows,
        "sample_rows": len(rows),
        "sample_limit": max_sample_rows,
        "sample_only": True,
        "quality_checked": False,
        "file_bytes": file_bytes,
    }


def _copy_raw_object(root: Path, source: Path, file_format: str) -> dict[str, Any]:
    source_hash = _sha256_file(source)
    relative = Path("raw") / f"sha256-{source_hash}.{file_format}"
    destination = _child(root, relative)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if not destination.is_file() or _sha256_file(destination) != source_hash:
            raise IntegrityError(f"raw object collision or mutation: {destination}")
    else:
        temporary = destination.parent / f".{destination.name}.{uuid4().hex}.tmp"
        try:
            with source.open("rb") as reader, temporary.open("xb") as writer:
                shutil.copyfileobj(reader, writer, length=1024 * 1024)
                writer.flush()
                os.fsync(writer.fileno())
            if _sha256_file(temporary) != source_hash:
                raise IntegrityError("raw copy failed source-hash verification")
            try:
                os.link(temporary, destination)
                temporary.unlink()
            except OSError:
                if destination.exists():
                    if _sha256_file(destination) != source_hash:
                        raise IntegrityError(f"raw object collision: {destination}")
                else:
                    os.replace(temporary, destination)
        finally:
            if temporary.exists():
                temporary.unlink()
    return {
        "path": relative.as_posix(),
        "sha256": source_hash,
        "bytes": destination.stat().st_size,
        "format": file_format,
        "original_name": source.name,
    }


def _read_source(path: Path, contract: Mapping[str, Any], max_rows: int) -> pd.DataFrame:
    input_spec = contract["input"]
    file_format = input_spec["format"]
    if file_format == "parquet":
        metadata_rows = pq.ParquetFile(path).metadata.num_rows
        if metadata_rows > max_rows:
            raise ResearchIntakeError(
                f"source rows {metadata_rows} exceed configured max_rows {max_rows}"
            )
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_csv(
            path,
            sep=input_spec["delimiter"],
            encoding=input_spec["encoding"],
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            nrows=max_rows + 1,
        )
    if len(frame) > max_rows:
        raise ResearchIntakeError(
            f"source rows {len(frame)} exceed configured max_rows {max_rows}"
        )
    if not frame.columns.is_unique:
        duplicates = frame.columns[frame.columns.duplicated()].tolist()
        raise ContractError(f"source contains duplicate column names: {duplicates}")
    missing = sorted(set(contract["mapping"].values()) - set(frame.columns))
    if missing:
        raise ContractError(f"mapped source columns are missing: {missing}")
    return frame


def _missing(values: pd.Series) -> pd.Series:
    result = values.isna()
    if pd.api.types.is_object_dtype(values.dtype) or pd.api.types.is_string_dtype(values.dtype):
        result |= values.astype("string").str.strip().eq("").fillna(True)
    return result


def _issue(
    ledger: list[dict[str, Any]],
    *,
    row: int | None,
    rule: str,
    column: str | None,
    value: Any,
    message: str,
    recommendation: str,
    severity: str = "error",
) -> None:
    ledger.append(
        {
            "row": row,
            "rule": rule,
            "column": column,
            "value": json.dumps(_jsonable(value), ensure_ascii=False, separators=(",", ":")),
            "severity": severity,
            "message": message,
            "recommendation": recommendation,
        }
    )


def _issues_for_mask(
    ledger: list[dict[str, Any]],
    mask: pd.Series,
    values: pd.Series,
    *,
    rule: str,
    column: str,
    message: str,
    recommendation: str,
) -> None:
    for position in values.index[mask]:
        _issue(
            ledger,
            row=int(position),
            rule=rule,
            column=column,
            value=values.loc[position],
            message=message,
            recommendation=recommendation,
        )


def _normalize(
    source: pd.DataFrame, contract: Mapping[str, Any]
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    mapped = pd.DataFrame(
        {canonical: source[source_name].copy() for canonical, source_name in contract["mapping"].items()}
    ).reset_index(drop=True)
    normalized = pd.DataFrame({_INTERNAL_ROW: pd.Series(range(len(mapped)), dtype="int64")})
    ledger: list[dict[str, Any]] = []
    metadata = contract["metadata"]
    source_format = contract["input"]["format"]
    for column, declared_type in contract["types"].items():
        values = mapped[column]
        missing = _missing(values)
        if declared_type == "string":
            converted = values.astype("string").mask(missing, pd.NA)
        elif declared_type in {"number", "integer"}:
            converted_numeric = pd.to_numeric(values.mask(missing), errors="coerce")
            invalid = ~missing & (converted_numeric.isna() | ~np.isfinite(converted_numeric))
            if declared_type == "integer":
                fractional = converted_numeric.notna() & converted_numeric.mod(1).ne(0)
                invalid |= fractional
                converted = converted_numeric.mask(invalid).astype("Int64")
            else:
                converted = converted_numeric.mask(invalid).astype("Float64")
            _issues_for_mask(
                ledger,
                invalid,
                values,
                rule=f"type_{declared_type}",
                column=column,
                message=f"value cannot be converted losslessly to {declared_type}",
                recommendation="correct the source value or change the explicit type contract",
            )
        elif declared_type in {"date", "datetime"}:
            date_format = contract["formats"].get(column)
            typed_parquet = source_format == "parquet" and (
                pd.api.types.is_datetime64_any_dtype(values.dtype)
                or all(isinstance(value, (date, datetime)) for value in values[~missing].head(20))
            )
            if not date_format and not typed_parquet:
                invalid = ~missing
                converted = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
                _issues_for_mask(
                    ledger,
                    invalid,
                    values,
                    rule="explicit_date_format",
                    column=column,
                    message="textual date/datetime has no explicit parsing format",
                    recommendation="declare formats.<column> using the source's exact date format",
                )
            else:
                converted = pd.to_datetime(
                    values.mask(missing), format=date_format, errors="coerce", exact=True
                )
                invalid = ~missing & converted.isna()
                _issues_for_mask(
                    ledger,
                    invalid,
                    values,
                    rule=f"type_{declared_type}",
                    column=column,
                    message=f"value does not match the declared {declared_type} format",
                    recommendation="correct the source value or its explicit date format",
                )
                if declared_type == "date":
                    if isinstance(converted.dtype, pd.DatetimeTZDtype):
                        timezone_values = ~converted.isna()
                        _issues_for_mask(
                            ledger,
                            timezone_values,
                            values,
                            rule="date_has_timezone",
                            column=column,
                            message="calendar date unexpectedly contains a time zone",
                            recommendation="map calendar dates separately from instant timestamps",
                        )
                        converted = converted.dt.tz_localize(None)
                    converted = converted.dt.normalize()
                else:
                    if not isinstance(converted.dtype, pd.DatetimeTZDtype):
                        declared_timezone = metadata.get("timezone")
                        if not isinstance(declared_timezone, str) or declared_timezone.lower() == "unknown":
                            timezone_values = ~missing & ~invalid
                            _issues_for_mask(
                                ledger,
                                timezone_values,
                                values,
                                rule="explicit_datetime_timezone",
                                column=column,
                                message="naive datetime has no explicit source timezone",
                                recommendation="declare metadata.timezone or include an offset in the source",
                            )
                            converted = pd.Series(
                                pd.NaT, index=values.index, dtype="datetime64[ns, UTC]"
                            )
                        else:
                            try:
                                converted = converted.dt.tz_localize(
                                    declared_timezone, ambiguous="NaT", nonexistent="NaT"
                                ).dt.tz_convert("UTC")
                            except (TypeError, ValueError) as exc:
                                raise ContractError(
                                    f"invalid or unsupported metadata.timezone: {declared_timezone}"
                                ) from exc
                    else:
                        converted = converted.dt.tz_convert("UTC")
        elif declared_type == "boolean":
            lowered = values.astype("string").str.strip().str.lower()
            mapping = {"true": True, "false": False, "1": True, "0": False}
            converted = lowered.map(mapping).astype("boolean")
            invalid = ~missing & converted.isna()
            _issues_for_mask(
                ledger,
                invalid,
                values,
                rule="type_boolean",
                column=column,
                message="boolean accepts only true, false, 1, or 0",
                recommendation="correct the source value or map it before import",
            )
        normalized[column] = converted

    required = set(contract["primary_key"])
    if contract["kind"] == "daily_bars":
        required |= {"symbol", "date", "open", "high", "low", "close"}
    elif contract["kind"] == "history":
        required |= {"symbol", "period_end"}
    for column in sorted(required):
        invalid = normalized[column].isna()
        _issues_for_mask(
            ledger,
            invalid,
            mapped[column],
            rule="missing_required_value",
            column=column,
            message="required canonical value is missing",
            recommendation="supply the required value; rows are never silently dropped or filled",
        )

    keys = contract["primary_key"]
    if keys:
        duplicate_mask = normalized.duplicated(keys, keep=False)
        if duplicate_mask.any():
            nonkeys = [column for column in contract["mapping"] if column not in keys]
            duplicate_rows = normalized.loc[duplicate_mask]
            for _, group in duplicate_rows.groupby(keys, dropna=False, sort=False):
                original_group = mapped.loc[group.index, nonkeys]
                conflicting = len(original_group.astype("string").drop_duplicates()) > 1
                rule = (
                    "duplicate_primary_key_conflict"
                    if conflicting
                    else "duplicate_primary_key"
                )
                message = (
                    "primary key is repeated with conflicting row values"
                    if conflicting
                    else "primary key is repeated"
                )
                for position in group.index:
                    _issue(
                        ledger,
                        row=int(position),
                        rule=rule,
                        column=",".join(keys),
                        value={key: mapped.loc[position, key] for key in keys},
                        message=message,
                        recommendation="resolve the source conflict explicitly; intake does not deduplicate",
                    )

    if contract["kind"] == "daily_bars":
        for column in ("open", "high", "low", "close"):
            invalid = normalized[column].notna() & normalized[column].le(0)
            _issues_for_mask(
                ledger,
                invalid,
                mapped[column],
                rule="positive_price",
                column=column,
                message="price must be positive",
                recommendation="correct the source price or document a different table kind",
            )
        if "volume" in normalized:
            invalid = normalized["volume"].notna() & normalized["volume"].lt(0)
            _issues_for_mask(
                ledger,
                invalid,
                mapped["volume"],
                rule="nonnegative_volume",
                column="volume",
                message="volume must be nonnegative",
                recommendation="correct the source volume and its declared unit",
            )
        complete = normalized[["open", "high", "low", "close"]].notna().all(axis=1)
        inconsistent = complete & (
            normalized["high"].lt(normalized[["open", "close", "low"]].max(axis=1))
            | normalized["low"].gt(normalized[["open", "close", "high"]].min(axis=1))
        )
        _issues_for_mask(
            ledger,
            inconsistent,
            mapped["high"],
            rule="ohlc_consistency",
            column="open,high,low,close",
            message="OHLC values violate high/low bounds",
            recommendation="correct the source bar; intake does not rewrite prices",
        )
    return normalized, ledger


def _ledger_frame(ledger: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    if not ledger:
        return pd.DataFrame(
            {
                "row": pd.Series(dtype="Int64"),
                "rule": pd.Series(dtype="string"),
                "column": pd.Series(dtype="string"),
                "value": pd.Series(dtype="string"),
                "severity": pd.Series(dtype="string"),
                "message": pd.Series(dtype="string"),
                "recommendation": pd.Series(dtype="string"),
            }
        )
    result = pd.DataFrame(ledger, columns=_ISSUE_COLUMNS)
    result["row"] = result["row"].astype("Int64")
    for column in _ISSUE_COLUMNS[1:]:
        result[column] = result[column].astype("string")
    return result


def _row_scope(frame: pd.DataFrame, rows: list[int]) -> dict[str, Any]:
    if not rows:
        return {"rows_with_issue": 0, "symbol_count": 0, "symbols": [], "date_min": None, "date_max": None}
    selected = frame.loc[frame[_INTERNAL_ROW].isin(rows)]
    symbols: list[str] = []
    if "symbol" in selected:
        symbols = sorted(selected["symbol"].dropna().astype(str).unique().tolist())
    date_column = "date" if "date" in selected else "period_end" if "period_end" in selected else None
    date_min = date_max = None
    if date_column is not None and selected[date_column].notna().any():
        date_min = pd.Timestamp(selected[date_column].min()).date().isoformat()
        date_max = pd.Timestamp(selected[date_column].max()).date().isoformat()
    return {
        "rows_with_issue": len(set(rows)),
        "symbol_count": len(symbols),
        "symbols": symbols[:MAX_ISSUE_SAMPLES],
        "date_min": date_min,
        "date_max": date_max,
    }


def _aggregate_issues(
    ledger: pd.DataFrame | Sequence[Mapping[str, Any]], frame: pd.DataFrame
) -> list[dict[str, Any]]:
    ledger_frame = ledger if isinstance(ledger, pd.DataFrame) else _ledger_frame(ledger)
    if ledger_frame.empty:
        return []
    groups = []
    keys = ["rule", "column", "severity", "message", "recommendation"]
    for key, group in ledger_frame.groupby(keys, dropna=False, sort=True):
        rule, column, severity, message, recommendation = key
        samples = []
        rows = []
        for item in group.head(MAX_ISSUE_SAMPLES).itertuples(index=False):
            row = None if pd.isna(item.row) else int(item.row)
            value = json.loads(item.value) if isinstance(item.value, str) else _jsonable(item.value)
            samples.append({"row": row, "column": None if pd.isna(column) else str(column), "value": value})
        for value in group["row"].dropna().tolist():
            rows.append(int(value))
        first = samples[0]
        issue_scope = _row_scope(frame, rows)
        issue_scope["columns"] = [] if pd.isna(column) else str(column).split(",")
        groups.append(
            {
                "rule": str(rule),
                "row": first["row"],
                "column": first["column"],
                "value": first["value"],
                "severity": str(severity),
                "message": str(message),
                "recommendation": str(recommendation),
                "affected_count": len(group),
                "scope": issue_scope,
                "samples": samples,
            }
        )
    return groups


def _coverage(frame: pd.DataFrame) -> dict[str, Any]:
    symbols = int(frame["symbol"].nunique(dropna=True)) if "symbol" in frame else None
    date_column = "date" if "date" in frame else "period_end" if "period_end" in frame else None
    start = end = None
    if date_column is not None and frame[date_column].notna().any():
        start = pd.Timestamp(frame[date_column].min()).date().isoformat()
        end = pd.Timestamp(frame[date_column].max()).date().isoformat()
    return {"rows": len(frame), "symbols": symbols, "start": start, "end": end}


def _report(frame: pd.DataFrame, ledger: list[dict[str, Any]]) -> dict[str, Any]:
    issues = _aggregate_issues(ledger, frame)
    status = "blocked" if any(issue["severity"] == "error" for issue in issues) else "ready"
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "full_scan": True,
        "total_rows": len(frame),
        "issue_count": len(ledger),
        "issue_group_count": len(issues),
        "issues": issues,
        "coverage": _coverage(frame),
        "sample_limit": MAX_ISSUE_SAMPLES,
        "samples_are_complete": all(
            issue["affected_count"] <= MAX_ISSUE_SAMPLES for issue in issues
        ),
    }


def _limits(contract: Mapping[str, Any], max_bytes: int | None, max_rows: int | None) -> tuple[int, int]:
    contract_limits = contract.get("limits", {})
    byte_limit = max_bytes if max_bytes is not None else contract_limits.get("max_bytes", DEFAULT_MAX_BYTES)
    row_limit = max_rows if max_rows is not None else contract_limits.get("max_rows", DEFAULT_MAX_ROWS)
    if not isinstance(byte_limit, int) or byte_limit <= 0:
        raise ContractError("effective max_bytes must be a positive integer")
    if not isinstance(row_limit, int) or row_limit <= 0:
        raise ContractError("effective max_rows must be a positive integer")
    return byte_limit, row_limit


def _receipt_path(root: Path, receipt_id: str) -> Path:
    return _child(root, Path("receipts") / f"{_safe_name(receipt_id, 'receipt')}.json")


def import_dataset(
    root: str | Path,
    source: str | Path,
    name: str,
    contract: Mapping[str, Any] | str | Path,
    *,
    parent: str | None = None,
    max_bytes: int | None = None,
    max_rows: int | None = None,
) -> dict[str, Any]:
    """Retain raw input, fully scan it, and atomically publish a ready/blocked version."""
    # Only mutation needs the shared cross-process lock.  Keeping this import
    # lazy makes direct read-snapshot/verify-snapshot execution self-contained.
    from quant_data_kit.process_lock import process_file_lock

    intake_root = _root_path(root, create=True)
    receipt_id = f"receipt-{uuid4().hex}"
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "id": receipt_id,
        "status": "pending",
        "name": str(name),
        "source": str(Path(source).absolute()),
        "created_at": _now(),
        "updated_at": _now(),
        "raw_sha256": None,
        "raw_path": None,
        "version_id": None,
        "error": None,
    }
    _atomic_write_json(_receipt_path(intake_root, receipt_id), receipt)
    raw_object: dict[str, Any] | None = None
    try:
        dataset_name = _safe_name(name, "dataset name")
        lexical_source = Path(source).absolute()
        if _is_reparse_point(lexical_source):
            raise ResearchIntakeError(
                f"source must be a regular non-symlink file: {lexical_source}"
            )
        source_path = lexical_source.resolve(strict=True)
        if not source_path.is_file():
            raise ResearchIntakeError(f"source must be a regular file: {source_path}")
        source_format = _resolve_format(source_path, None)
        lock_path = _child(intake_root, ".research-intake.lock")
        with process_file_lock(lock_path):
            raw_object = _copy_raw_object(intake_root, source_path, source_format)
            receipt.update(
                raw_sha256=raw_object["sha256"],
                raw_path=raw_object["path"],
                updated_at=_now(),
            )
            _atomic_write_json(_receipt_path(intake_root, receipt_id), receipt)
            explicit_contract = _load_contract(contract)
            if explicit_contract["input"]["format"] != source_format:
                raise ContractError("source extension and contract input.format differ")
            byte_limit, row_limit = _limits(explicit_contract, max_bytes, max_rows)
            if raw_object["bytes"] > byte_limit:
                raise ResearchIntakeError(
                    f"source bytes {raw_object['bytes']} exceed configured max_bytes {byte_limit}"
                )
            catalog = _catalog(intake_root)
            dataset = catalog["datasets"].get(dataset_name)
            current_latest = dataset.get("latest") if dataset else None
            if parent is not None:
                _safe_name(parent, "parent version")
                if parent != current_latest:
                    raise ResearchIntakeError(
                        f"parent conflict: expected current latest {current_latest!r}, received {parent!r}"
                    )
            selected_parent = parent if parent is not None else current_latest
            retained_raw = _child(intake_root, raw_object["path"], must_exist=True)
            source_frame = _read_source(retained_raw, explicit_contract, row_limit)
            normalized, ledger = _normalize(source_frame, explicit_contract)
            report = _report(normalized, ledger)
            staging_root = _child(intake_root, ".staging")
            staging_root.mkdir(parents=True, exist_ok=True)
            staging = staging_root / f"version-{uuid4().hex}"
            staging.mkdir()
            try:
                raw_dir = staging / "raw"
                raw_dir.mkdir()
                bundled_raw = raw_dir / f"source.{source_format}"
                shutil.copyfile(retained_raw, bundled_raw)
                if _sha256_file(bundled_raw) != raw_object["sha256"]:
                    raise IntegrityError("bundled raw copy does not match retained raw hash")
                normalized_path = staging / "normalized.parquet"
                issues_path = staging / "issues.parquet"
                contract_path = staging / "contract.json"
                report_path = staging / "report.json"
                normalized.to_parquet(normalized_path, index=False)
                _ledger_frame(ledger).to_parquet(issues_path, index=False)
                contract_path.write_bytes(_canonical_bytes(explicit_contract))
                report_path.write_bytes(_canonical_bytes(report))
                files = {}
                for key, path in {
                    "normalized": normalized_path,
                    "contract": contract_path,
                    "report": report_path,
                    "issues": issues_path,
                }.items():
                    files[key] = {
                        "path": path.name,
                        "sha256": _sha256_file(path),
                        "bytes": path.stat().st_size,
                    }
                created_at = _now()
                raw_manifest = {
                    "path": f"raw/{bundled_raw.name}",
                    "sha256": raw_object["sha256"],
                    "bytes": raw_object["bytes"],
                    "format": source_format,
                    "original_name": raw_object["original_name"],
                    "source_object_path": raw_object["path"],
                }
                manifest_identity = {
                    "schema_version": MANIFEST_SCHEMA_VERSION,
                    "name": dataset_name,
                    "status": report["status"],
                    "parent_id": selected_parent,
                    "created_at": created_at,
                    "row_count": len(normalized),
                    "columns": sorted(explicit_contract["mapping"]),
                    "types": explicit_contract["types"],
                    "coverage": report["coverage"],
                    "raw": raw_manifest,
                    "files": files,
                }
                identity_hash = hashlib.sha256(_canonical_bytes(manifest_identity)).hexdigest()
                version_id = f"sha256-{identity_hash}"
                manifest = {
                    "id": version_id,
                    "identity_sha256": identity_hash,
                    **manifest_identity,
                }
                (staging / "manifest.json").write_bytes(_canonical_bytes(manifest))
                versions_root = _child(intake_root, "versions")
                versions_root.mkdir(parents=True, exist_ok=True)
                destination = versions_root / version_id
                if destination.exists():
                    existing = _read_json(destination / "manifest.json")
                    if existing != manifest:
                        raise IntegrityError(f"immutable version collision: {version_id}")
                    shutil.rmtree(staging)
                else:
                    os.replace(staging, destination)
                version_summary = {
                    "id": version_id,
                    "status": report["status"],
                    "parent_id": selected_parent,
                    "created_at": created_at,
                    "rows": len(normalized),
                    "raw_sha256": raw_object["sha256"],
                    "normalized_sha256": files["normalized"]["sha256"],
                }
                if dataset is None:
                    dataset = {"latest": None, "versions": []}
                    catalog["datasets"][dataset_name] = dataset
                if not any(item["id"] == version_id for item in dataset["versions"]):
                    dataset["versions"].append(version_summary)
                if report["status"] == "ready":
                    dataset["latest"] = version_id
                _atomic_write_json(_child(intake_root, "catalog.json"), catalog)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
            receipt.update(
                status="succeeded",
                version_id=version_id,
                updated_at=_now(),
            )
            _atomic_write_json(_receipt_path(intake_root, receipt_id), receipt)
            version_public = {
                **manifest,
                "raw_sha256": raw_manifest["sha256"],
                "normalized_sha256": files["normalized"]["sha256"],
            }
            return {"receipt": receipt, "version": version_public, "report": report}
    except Exception as exc:  # noqa: BLE001 - every failed attempt must leave a receipt
        receipt.update(
            status="failed",
            updated_at=_now(),
            error={"type": type(exc).__name__, "message": str(exc)},
        )
        if raw_object is not None:
            receipt["raw_sha256"] = raw_object["sha256"]
            receipt["raw_path"] = raw_object["path"]
        _atomic_write_json(_receipt_path(intake_root, receipt_id), receipt)
        return {"receipt": receipt, "version": None, "report": None, "error": receipt["error"]}


def list_datasets(root: str | Path, *, name: str | None = None) -> dict[str, Any]:
    intake_root = _root_path(root, create=False)
    catalog = _catalog(intake_root)
    if name is not None:
        _safe_name(name, "dataset name")
        names = [name] if name in catalog["datasets"] else []
    else:
        names = sorted(catalog["datasets"])
    datasets = []
    for dataset_name in names:
        item = catalog["datasets"][dataset_name]
        versions = list(item.get("versions", []))
        datasets.append(
            {
                "name": dataset_name,
                "latest": item.get("latest"),
                "version_count": len(versions),
                "versions": versions,
            }
        )
    return {"datasets": datasets}


def _resolve_version(root: Path, name: str, version: str | None) -> tuple[str, Path]:
    dataset_name = _safe_name(name, "dataset name")
    catalog = _catalog(root)
    dataset = catalog["datasets"].get(dataset_name)
    if dataset is None:
        raise ResearchIntakeError(f"unknown dataset: {dataset_name}")
    selected = version or dataset.get("latest")
    if selected is None:
        raise ResearchIntakeError(f"dataset has no ready latest version: {dataset_name}")
    _safe_name(selected, "version")
    known = {item["id"] for item in dataset.get("versions", [])}
    if selected not in known:
        raise ResearchIntakeError(f"version does not belong to dataset {dataset_name}: {selected}")
    directory = _child(root, Path("versions") / selected, must_exist=True)
    manifest = _read_json(directory / "manifest.json")
    if manifest.get("name") != dataset_name:
        raise IntegrityError("catalog dataset name differs from immutable version manifest")
    return selected, directory


def verify_snapshot(directory: str | Path) -> dict[str, Any]:
    """Verify a self-contained copied version directory without its catalog."""
    lexical_snapshot = Path(directory).absolute()
    if _is_reparse_point(lexical_snapshot):
        raise IntegrityError(f"snapshot cannot be a symlink or reparse point: {lexical_snapshot}")
    snapshot = lexical_snapshot.resolve(strict=True)
    if not snapshot.is_dir():
        raise IntegrityError(f"snapshot is not a regular directory: {snapshot}")
    manifest = _read_json(snapshot / "manifest.json")
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise IntegrityError("unsupported research-intake version schema")
    required_manifest = {
        "id",
        "identity_sha256",
        "name",
        "status",
        "parent_id",
        "created_at",
        "row_count",
        "columns",
        "types",
        "coverage",
        "raw",
        "files",
    }
    missing_manifest = sorted(required_manifest - set(manifest))
    if missing_manifest:
        raise IntegrityError(f"version manifest lacks fields: {missing_manifest}")
    if manifest["status"] not in {"ready", "blocked"}:
        raise IntegrityError("version manifest status must be ready or blocked")
    if not isinstance(manifest["row_count"], int) or manifest["row_count"] < 0:
        raise IntegrityError("version manifest row_count must be nonnegative")
    if (
        not isinstance(manifest["columns"], list)
        or not all(isinstance(column, str) for column in manifest["columns"])
        or len(manifest["columns"]) != len(set(manifest["columns"]))
    ):
        raise IntegrityError("version manifest columns must be unique strings")
    identity = {
        key: value for key, value in manifest.items() if key not in {"id", "identity_sha256"}
    }
    identity_hash = hashlib.sha256(_canonical_bytes(identity)).hexdigest()
    if manifest.get("identity_sha256") != identity_hash or manifest.get("id") != f"sha256-{identity_hash}":
        raise IntegrityError("version manifest identity mismatch")
    raw = manifest.get("raw")
    if not isinstance(raw, dict):
        raise IntegrityError("version manifest lacks raw evidence")
    expected_files = manifest.get("files")
    if not isinstance(expected_files, dict) or set(expected_files) != {
        "normalized",
        "contract",
        "report",
        "issues",
    }:
        raise IntegrityError("version manifest files are incomplete")
    entries = {"raw": raw, **expected_files}
    verified = {}
    for key, entry in entries.items():
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise IntegrityError(f"invalid {key} manifest entry")
        path = _child(snapshot, entry["path"], must_exist=True)
        if not path.is_file():
            raise IntegrityError(f"{key} evidence is not a file")
        actual = _sha256_file(path)
        if actual != entry.get("sha256"):
            raise IntegrityError(f"{key} sha256 mismatch")
        if path.stat().st_size != entry.get("bytes"):
            raise IntegrityError(f"{key} byte-length mismatch")
        verified[key] = actual
    contract = _load_contract(snapshot / expected_files["contract"]["path"])
    report = _read_json(snapshot / expected_files["report"]["path"])
    if report.get("schema_version") != REPORT_SCHEMA_VERSION or report.get("full_scan") is not True:
        raise IntegrityError("version report is not a complete full-scan report")
    if report.get("total_rows") != manifest.get("row_count"):
        raise IntegrityError("version report row count differs from manifest")
    if report.get("status") != manifest.get("status"):
        raise IntegrityError("version report status differs from manifest")
    normalized_parquet = pq.ParquetFile(snapshot / expected_files["normalized"]["path"])
    expected_normalized_columns = {_INTERNAL_ROW, *manifest["columns"]}
    if (
        normalized_parquet.metadata.num_rows != manifest.get("row_count")
        or set(normalized_parquet.schema_arrow.names) != expected_normalized_columns
    ):
        raise IntegrityError("normalized content does not match manifest row contract")
    if sorted(contract["mapping"]) != manifest.get("columns") or contract["types"] != manifest.get("types"):
        raise IntegrityError("contract schema differs from manifest")
    issues_parquet = pq.ParquetFile(snapshot / expected_files["issues"]["path"])
    if set(issues_parquet.schema_arrow.names) != set(_ISSUE_COLUMNS):
        raise IntegrityError("issue ledger schema is incomplete")
    if issues_parquet.metadata.num_rows != report.get("issue_count"):
        raise IntegrityError("issue ledger count differs from report")
    return {
        "valid": True,
        "version": manifest,
        "contract": contract,
        "report": report,
        "hashes": verified,
    }


def _snapshot_payload(directory: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], pd.DataFrame, pd.DataFrame]:
    verified = verify_snapshot(directory)
    manifest = verified["version"]
    contract = verified["contract"]
    report = verified["report"]
    normalized = pd.read_parquet(directory / manifest["files"]["normalized"]["path"])
    issues = pd.read_parquet(directory / manifest["files"]["issues"]["path"])
    return manifest, contract, report, normalized, issues


def show_dataset(
    root: str | Path,
    *,
    name: str | None = None,
    version: str | None = None,
    receipt: str | None = None,
) -> dict[str, Any]:
    intake_root = _root_path(root, create=False)
    if receipt is not None:
        if name is not None or version is not None:
            raise ResearchIntakeError("receipt cannot be combined with name or version")
        payload = _read_json(_receipt_path(intake_root, receipt))
        raw_path = payload.get("raw_path")
        if isinstance(raw_path, str) and isinstance(payload.get("raw_sha256"), str):
            retained = _child(intake_root, raw_path, must_exist=True)
            actual = _sha256_file(retained)
            payload["integrity"] = {
                "valid": actual == payload["raw_sha256"],
                "expected_raw_sha256": payload["raw_sha256"],
                "actual_raw_sha256": actual,
            }
        else:
            payload["integrity"] = {"valid": None, "reason": "no retained raw object"}
        return payload
    if name is None:
        raise ResearchIntakeError("show requires name or receipt")
    _, directory = _resolve_version(intake_root, name, version)
    verified = verify_snapshot(directory)
    return {
        "version": verified["version"],
        "contract": verified["contract"],
        "report": verified["report"],
        "integrity": {"valid": True, "hashes": verified["hashes"]},
    }


def _parse_boundary(value: str | None, field: str) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        parsed = pd.Timestamp(value)
    except ValueError as exc:
        raise ResearchIntakeError(f"{field} must be an ISO calendar date") from exc
    if parsed.tzinfo is not None or parsed != parsed.normalize():
        raise ResearchIntakeError(f"{field} must be a timezone-naive ISO calendar date")
    return parsed


def _scope_masks(
    frame: pd.DataFrame,
    *,
    symbols: Sequence[str] | None,
    start: str | None,
    end: str | None,
) -> tuple[pd.Series, pd.Series, pd.Timestamp | None, pd.Timestamp | None, str | None]:
    selected = pd.Series(True, index=frame.index)
    potential = pd.Series(True, index=frame.index)
    if symbols is not None:
        if "symbol" not in frame:
            raise ResearchIntakeError("symbol scope requires a canonical symbol column")
        wanted = {str(value) for value in symbols}
        if not wanted:
            raise ResearchIntakeError("symbols scope cannot be empty")
        match = frame["symbol"].astype("string").isin(wanted)
        selected &= match
        potential &= match | frame["symbol"].isna()
    parsed_start = _parse_boundary(start, "start")
    parsed_end = _parse_boundary(end, "end")
    if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
        raise ResearchIntakeError("start cannot be after end")
    date_column = "date" if "date" in frame else "period_end" if "period_end" in frame else None
    if (parsed_start is not None or parsed_end is not None) and date_column is None:
        raise ResearchIntakeError("date scope requires canonical date or period_end")
    if parsed_start is not None:
        match = frame[date_column].ge(parsed_start)
        selected &= match.fillna(False)
        potential &= match.fillna(True)
    if parsed_end is not None:
        match = frame[date_column].le(parsed_end)
        selected &= match.fillna(False)
        potential &= match.fillna(True)
    return selected, potential, parsed_start, parsed_end, date_column


def _semantic_ledger(
    contract: Mapping[str, Any],
    frame: pd.DataFrame,
    selected: pd.Series,
    *,
    purpose: str,
    columns: list[str],
    symbols: Sequence[str] | None,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    date_column: str | None,
) -> list[dict[str, Any]]:
    ledger: list[dict[str, Any]] = []

    def global_issue(rule: str, column: str | None, message: str, recommendation: str) -> None:
        _issue(
            ledger,
            row=None,
            rule=rule,
            column=column,
            value=None,
            message=message,
            recommendation=recommendation,
        )

    if purpose == "daily_bars_research":
        if contract["kind"] != "daily_bars":
            global_issue(
                "purpose_table_kind",
                None,
                "daily-bars research requires a daily_bars contract",
                "import the source with an explicit daily_bars contract",
            )
        for field in ("symbol", "date"):
            if field not in contract["mapping"]:
                global_issue(
                    "purpose_required_column",
                    field,
                    f"daily-bars research requires canonical {field}",
                    f"map and type the {field} column explicitly",
                )
        metadata = contract["metadata"]
        for field in ("source", "provider", "timezone", "adjustment"):
            value = metadata.get(field)
            if not isinstance(value, str) or not value.strip() or value.lower() == "unknown":
                global_issue(
                    f"declared_{field}",
                    None,
                    f"daily-bars research requires explicit metadata.{field}",
                    f"declare metadata.{field}; intake does not infer it",
                )
        units = metadata.get("units", {})
        for column in columns:
            if contract["types"].get(column) in {"integer", "number"}:
                value = units.get(column)
                if not isinstance(value, str) or not value.strip() or value.lower() == "unknown":
                    global_issue(
                        "declared_units",
                        column,
                        f"strict use of {column} requires an explicit unit",
                        f"declare metadata.units.{column}; intake does not infer units",
                    )
    elif purpose == "historical_financial_factor_backtest":
        if contract["kind"] != "history":
            global_issue(
                "purpose_table_kind",
                None,
                "historical financial-factor replay requires a history contract",
                "import the source with an explicit history contract",
            )
        if contract["types"].get("available_at") != "datetime":
            global_issue(
                "historical_available_at",
                "available_at",
                "historical financial-factor replay requires typed disclosure availability",
                "map the actual disclosure timestamp to available_at as datetime",
            )
        else:
            missing = selected & frame["available_at"].isna()
            for position in frame.index[missing]:
                _issue(
                    ledger,
                    row=int(frame.loc[position, _INTERNAL_ROW]),
                    rule="historical_available_at",
                    column="available_at",
                    value=None,
                    message="historical row lacks disclosure availability time",
                    recommendation="supply actual disclosure availability; do not backdate it",
                )
            if "period_end" in frame:
                period_end = pd.to_datetime(frame["period_end"], errors="coerce", utc=True)
                premature = (
                    selected
                    & frame["available_at"].notna()
                    & period_end.notna()
                    & frame["available_at"].lt(period_end)
                )
                for position in frame.index[premature]:
                    _issue(
                        ledger,
                        row=int(frame.loc[position, _INTERNAL_ROW]),
                        rule="available_at_order",
                        column="available_at",
                        value=frame.loc[position, "available_at"],
                        message="disclosure availability precedes its financial period end",
                        recommendation="supply actual disclosure availability evidence",
                    )
        if contract["metadata"].get("availability") != "point_in_time":
            global_issue(
                "point_in_time_availability",
                "available_at",
                "source does not declare point-in-time availability semantics",
                "use actual vintage/disclosure evidence and declare availability=point_in_time",
            )
        units = contract["metadata"].get("units", {})
        for column in columns:
            if contract["types"].get(column) in {"integer", "number"}:
                value = units.get(column)
                if not isinstance(value, str) or not value.strip() or value.lower() == "unknown":
                    global_issue(
                        "declared_units",
                        column,
                        f"strict use of {column} requires an explicit unit",
                        f"declare metadata.units.{column}; intake does not infer units",
                    )
    if symbols is not None and "symbol" in frame:
        present = set(frame.loc[selected, "symbol"].dropna().astype(str))
        missing_symbols = sorted(set(map(str, symbols)) - present)
        for symbol in missing_symbols:
            global_issue(
                "requested_symbol_coverage",
                "symbol",
                f"requested symbol has no rows in scope: {symbol}",
                "adjust the requested scope or provide complete source coverage",
            )
    if (start is not None or end is not None) and date_column is not None and not selected.any():
        global_issue(
            "requested_date_coverage",
            date_column,
            "requested date scope contains no rows",
            "adjust the requested scope or provide complete source coverage",
        )
    return ledger


def _check_snapshot(
    directory: Path,
    *,
    purpose: str,
    columns: Sequence[str] | None,
    symbols: Sequence[str] | None,
    start: str | None,
    end: str | None,
) -> tuple[dict[str, Any], pd.DataFrame, pd.Series, dict[str, Any]]:
    if purpose not in SUPPORTED_PURPOSES:
        raise ResearchIntakeError(f"unsupported purpose: {purpose}")
    manifest, contract, _, frame, issues = _snapshot_payload(directory)
    canonical_columns = list(contract["mapping"])
    selected_columns = canonical_columns if columns is None else list(dict.fromkeys(columns))
    if not selected_columns:
        raise ResearchIntakeError("columns scope cannot be empty")
    unknown = sorted(set(selected_columns) - set(canonical_columns))
    if unknown:
        raise ResearchIntakeError(f"requested canonical columns are unknown: {unknown}")
    selected, potential, parsed_start, parsed_end, date_column = _scope_masks(
        frame, symbols=symbols, start=start, end=end
    )
    if issues.empty:
        scoped_issues = issues.copy()
    else:
        row_numbers = set(frame.loc[potential, _INTERNAL_ROW].astype(int))
        structural_rules = {"duplicate_primary_key", "duplicate_primary_key_conflict"}
        scope_columns = {"symbol", "date", "period_end"}

        def relevant_issue(item: pd.Series) -> bool:
            if item["rule"] in structural_rules or pd.isna(item["column"]):
                return True
            issue_columns = set(str(item["column"]).split(","))
            return bool(issue_columns & (set(selected_columns) | scope_columns))

        column_match = issues.apply(relevant_issue, axis=1)
        row_match = issues["row"].isna() | issues["row"].isin(row_numbers)
        scoped_issues = issues.loc[column_match & row_match].copy()
    semantic = _semantic_ledger(
        contract,
        frame,
        selected,
        purpose=purpose,
        columns=selected_columns,
        symbols=symbols,
        start=parsed_start,
        end=parsed_end,
        date_column=date_column,
    )
    combined = pd.concat([scoped_issues, _ledger_frame(semantic)], ignore_index=True)
    aggregated = _aggregate_issues(combined, frame)
    allowed = not any(issue["severity"] == "error" for issue in aggregated)
    full_scan = columns is None and symbols is None and start is None and end is None
    scope = {
        "columns": selected_columns,
        "symbols": list(map(str, symbols)) if symbols is not None else None,
        "start": start,
        "end": end,
        "full_scan": full_scan,
        "total_rows": len(frame),
        "matched_rows": int(selected.sum()),
    }
    result = {
        "allowed": allowed,
        "status": "ready" if allowed else "blocked",
        "purpose": purpose,
        "version_id": manifest["id"],
        "scope": scope,
        "integrity": {
            "valid": True,
            "raw_sha256": manifest["raw"]["sha256"],
            "normalized_sha256": manifest["files"]["normalized"]["sha256"],
            "contract_sha256": manifest["files"]["contract"]["sha256"],
            "report_sha256": manifest["files"]["report"]["sha256"],
            "issues_sha256": manifest["files"]["issues"]["sha256"],
        },
        "issues": aggregated,
        "market_certified": False,
        "certification": "contract-and-data-checks-only",
    }
    return result, frame, selected, manifest


def check_snapshot(
    directory: str | Path,
    *,
    purpose: str = "exploration",
    columns: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    return _check_snapshot(
        Path(directory).absolute(),
        purpose=purpose,
        columns=columns,
        symbols=symbols,
        start=start,
        end=end,
    )[0]


def check_dataset(
    root: str | Path,
    name: str,
    *,
    version: str | None = None,
    purpose: str = "exploration",
    columns: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    intake_root = _root_path(root, create=False)
    _, directory = _resolve_version(intake_root, name, version)
    return check_snapshot(
        directory,
        purpose=purpose,
        columns=columns,
        symbols=symbols,
        start=start,
        end=end,
    )


def read_snapshot(
    directory: str | Path,
    *,
    purpose: str = "exploration",
    columns: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> IntakeReadResult:
    snapshot = Path(directory).absolute()
    check, frame, selected, manifest = _check_snapshot(
        snapshot,
        purpose=purpose,
        columns=columns,
        symbols=symbols,
        start=start,
        end=end,
    )
    if not check["allowed"]:
        rules = sorted({issue["rule"] for issue in check["issues"]})
        raise AdmissionError(f"scope is blocked for {purpose}: {rules}")
    selected_columns = check["scope"]["columns"]
    output = frame.loc[selected, selected_columns].reset_index(drop=True)
    metadata = {
        "version_id": manifest["id"],
        "name": manifest["name"],
        "raw_sha256": manifest["raw"]["sha256"],
        "normalized_sha256": manifest["files"]["normalized"]["sha256"],
        "contract_sha256": manifest["files"]["contract"]["sha256"],
        "report_sha256": manifest["files"]["report"]["sha256"],
        "issues_sha256": manifest["files"]["issues"]["sha256"],
        "market_certified": False,
    }
    return IntakeReadResult(frame=output, metadata=metadata, check=check)


def read_dataset(
    root: str | Path,
    name: str,
    *,
    version: str | None = None,
    purpose: str = "exploration",
    columns: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> IntakeReadResult:
    intake_root = _root_path(root, create=False)
    _, directory = _resolve_version(intake_root, name, version)
    return read_snapshot(
        directory,
        purpose=purpose,
        columns=columns,
        symbols=symbols,
        start=start,
        end=end,
    )


def _record_payload(row: pd.Series, columns: Sequence[str]) -> bytes:
    return _canonical_bytes({column: _jsonable(row[column]) for column in columns})


def _diff_coverage(frame: pd.DataFrame) -> dict[str, Any]:
    public = frame.drop(columns=[_INTERNAL_ROW], errors="ignore")
    return _coverage(public)


def diff_versions(
    root: str | Path,
    name: str,
    old: str,
    new: str,
    *,
    keys: Sequence[str] | None = None,
    sample_limit: int = 20,
) -> dict[str, Any]:
    if not 0 <= sample_limit <= 20:
        raise ResearchIntakeError("sample_limit must be between 0 and 20")
    intake_root = _root_path(root, create=False)
    _, old_dir = _resolve_version(intake_root, name, old)
    _, new_dir = _resolve_version(intake_root, name, new)
    old_manifest, old_contract, _, old_frame, _ = _snapshot_payload(old_dir)
    new_manifest, new_contract, _, new_frame, _ = _snapshot_payload(new_dir)
    old_columns = list(old_contract["mapping"])
    new_columns = list(new_contract["mapping"])
    common = [column for column in old_columns if column in new_columns]
    selected_keys = list(keys) if keys is not None else list(old_contract["primary_key"])
    if keys is None and selected_keys != list(new_contract["primary_key"]):
        raise ResearchIntakeError("version primary keys differ; pass explicit diff keys")
    if not selected_keys:
        old_counter = Counter(_record_payload(row, old_columns) for _, row in old_frame.iterrows())
        new_counter = Counter(_record_payload(row, new_columns) for _, row in new_frame.iterrows())
        unchanged = sum((old_counter & new_counter).values())
        rows = {
            "added": sum((new_counter - old_counter).values()),
            "removed": sum((old_counter - new_counter).values()),
            "changed": 0,
            "unchanged": unchanged,
        }
        samples: list[dict[str, Any]] = []
    else:
        unknown = sorted((set(selected_keys) - set(old_columns)) | (set(selected_keys) - set(new_columns)))
        if unknown:
            raise ResearchIntakeError(f"diff keys are missing from one version: {unknown}")
        if old_frame.duplicated(selected_keys).any() or new_frame.duplicated(selected_keys).any():
            raise ResearchIntakeError("diff keys are not unique in one or both versions")
        old_by_key = {
            tuple(_jsonable(row[key]) for key in selected_keys): row
            for _, row in old_frame.iterrows()
        }
        new_by_key = {
            tuple(_jsonable(row[key]) for key in selected_keys): row
            for _, row in new_frame.iterrows()
        }
        added_keys = new_by_key.keys() - old_by_key.keys()
        removed_keys = old_by_key.keys() - new_by_key.keys()
        changed_keys = []
        unchanged = 0
        compare_columns = [column for column in common if column not in selected_keys]
        for key in old_by_key.keys() & new_by_key.keys():
            old_payload = _record_payload(old_by_key[key], compare_columns)
            new_payload = _record_payload(new_by_key[key], compare_columns)
            if old_payload == new_payload:
                unchanged += 1
            else:
                changed_keys.append(key)
        rows = {
            "added": len(added_keys),
            "removed": len(removed_keys),
            "changed": len(changed_keys),
            "unchanged": unchanged,
        }
        samples = []
        for change, selected in (
            ("added", sorted(added_keys, key=repr)),
            ("removed", sorted(removed_keys, key=repr)),
            ("changed", sorted(changed_keys, key=repr)),
        ):
            for key in selected:
                if len(samples) >= sample_limit:
                    break
                samples.append(
                    {
                        "change": change,
                        "key": {field: value for field, value in zip(selected_keys, key, strict=True)},
                    }
                )
    schema = {
        "added_columns": sorted(set(new_columns) - set(old_columns)),
        "removed_columns": sorted(set(old_columns) - set(new_columns)),
        "type_changes": {
            column: {"old": old_contract["types"][column], "new": new_contract["types"][column]}
            for column in common
            if old_contract["types"][column] != new_contract["types"][column]
        },
        "mapping_changes": {
            column: {
                "old": old_contract["mapping"][column],
                "new": new_contract["mapping"][column],
            }
            for column in common
            if old_contract["mapping"][column] != new_contract["mapping"][column]
        },
    }
    return {
        "name": name,
        "old": old_manifest["id"],
        "new": new_manifest["id"],
        "keys": selected_keys,
        "rows": rows,
        "schema": schema,
        "coverage": {"old": _diff_coverage(old_frame), "new": _diff_coverage(new_frame)},
        "samples": samples,
        "sample_limit": sample_limit,
        "counts_are_complete": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)

    inspect_parser = subparsers.add_parser("inspect-source")
    inspect_parser.add_argument("--source", required=True)
    inspect_parser.add_argument("--format", choices=sorted(SUPPORTED_FORMATS))
    inspect_parser.add_argument("--encoding", default="utf-8")
    inspect_parser.add_argument("--delimiter")
    inspect_parser.add_argument("--max-sample-rows", type=int, default=20)
    inspect_parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)

    import_parser = subparsers.add_parser("import")
    import_parser.add_argument("--root", required=True)
    import_parser.add_argument("--source", required=True)
    import_parser.add_argument("--name", required=True)
    import_parser.add_argument("--contract", required=True)
    import_parser.add_argument("--parent")
    import_parser.add_argument("--max-bytes", type=int)
    import_parser.add_argument("--max-rows", type=int)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--root", required=True)
    list_parser.add_argument("--name")

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("--root", required=True)
    show_parser.add_argument("--name")
    show_parser.add_argument("--version")
    show_parser.add_argument("--receipt")

    for action in ("check", "read"):
        action_parser = subparsers.add_parser(action)
        action_parser.add_argument("--root", required=True)
        action_parser.add_argument("--name", required=True)
        action_parser.add_argument("--version")
        action_parser.add_argument("--purpose", choices=sorted(SUPPORTED_PURPOSES), default="exploration")
        action_parser.add_argument("--columns", nargs="+")
        action_parser.add_argument("--symbols", nargs="+")
        action_parser.add_argument("--start")
        action_parser.add_argument("--end")
        if action == "read":
            action_parser.add_argument("--limit", type=int, default=1000)
            action_parser.add_argument("--max-rows", type=int)

    verify_snapshot_parser = subparsers.add_parser("verify-snapshot")
    verify_snapshot_parser.add_argument("--snapshot", required=True)

    for action in ("check-snapshot", "read-snapshot"):
        snapshot_parser = subparsers.add_parser(action)
        snapshot_parser.add_argument("--snapshot", required=True)
        snapshot_parser.add_argument(
            "--purpose", choices=sorted(SUPPORTED_PURPOSES), default="exploration"
        )
        snapshot_parser.add_argument("--columns", nargs="+")
        snapshot_parser.add_argument("--symbols", nargs="+")
        snapshot_parser.add_argument("--start")
        snapshot_parser.add_argument("--end")
        if action == "read-snapshot":
            snapshot_parser.add_argument("--limit", type=int, default=1000)
            snapshot_parser.add_argument("--max-rows", type=int)

    diff_parser = subparsers.add_parser("diff")
    diff_parser.add_argument("--root", required=True)
    diff_parser.add_argument("--name", required=True)
    diff_parser.add_argument("--old", required=True)
    diff_parser.add_argument("--new", required=True)
    diff_parser.add_argument("--key", nargs="+")
    diff_parser.add_argument("--sample-limit", type=int, default=20)
    return parser


def _scope_arguments(
    arguments: argparse.Namespace, *, include_version: bool
) -> dict[str, Any]:
    scope = {
        "purpose": arguments.purpose,
        "columns": arguments.columns,
        "symbols": arguments.symbols,
        "start": arguments.start,
        "end": arguments.end,
    }
    if include_version:
        scope["version"] = arguments.version
    return scope


def _read_cli_data(result: IntakeReadResult, *, limit: int, max_rows: int | None) -> dict[str, Any]:
    if limit < 0:
        raise ResearchIntakeError("read limit cannot be negative")
    if max_rows is not None and max_rows <= 0:
        raise ResearchIntakeError("read max_rows must be positive")
    matched = len(result.frame)
    if max_rows is not None and matched > max_rows:
        raise ResearchIntakeError(
            f"matched rows {matched} exceed configured read max_rows {max_rows}"
        )
    returned = matched if limit == 0 else min(matched, limit)
    rows = result.frame.head(returned).to_dict(orient="records")
    return {
        **result.metadata,
        "check": result.check,
        "rows": [_jsonable(row) for row in rows],
        "matched_rows": matched,
        "returned_rows": returned,
        "complete": returned == matched,
        "truncated": returned != matched,
        "max_rows": max_rows,
    }


def _run_action(arguments: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    if arguments.action == "inspect-source":
        data = inspect_source(
            arguments.source,
            file_format=arguments.format,
            encoding=arguments.encoding,
            delimiter=arguments.delimiter,
            max_sample_rows=arguments.max_sample_rows,
            max_bytes=arguments.max_bytes,
        )
    elif arguments.action == "import":
        data = import_dataset(
            arguments.root,
            arguments.source,
            arguments.name,
            arguments.contract,
            parent=arguments.parent,
            max_bytes=arguments.max_bytes,
            max_rows=arguments.max_rows,
        )
        return data, data["version"] is not None
    elif arguments.action == "list":
        data = list_datasets(arguments.root, name=arguments.name)
    elif arguments.action == "show":
        data = show_dataset(
            arguments.root,
            name=arguments.name,
            version=arguments.version,
            receipt=arguments.receipt,
        )
    elif arguments.action == "check":
        data = check_dataset(
            arguments.root,
            arguments.name,
            **_scope_arguments(arguments, include_version=True),
        )
    elif arguments.action == "read":
        result = read_dataset(
            arguments.root,
            arguments.name,
            **_scope_arguments(arguments, include_version=True),
        )
        data = _read_cli_data(result, limit=arguments.limit, max_rows=arguments.max_rows)
    elif arguments.action == "verify-snapshot":
        data = verify_snapshot(arguments.snapshot)
    elif arguments.action == "check-snapshot":
        data = check_snapshot(
            arguments.snapshot,
            **_scope_arguments(arguments, include_version=False),
        )
    elif arguments.action == "read-snapshot":
        result = read_snapshot(
            arguments.snapshot,
            **_scope_arguments(arguments, include_version=False),
        )
        data = _read_cli_data(result, limit=arguments.limit, max_rows=arguments.max_rows)
    elif arguments.action == "diff":
        data = diff_versions(
            arguments.root,
            arguments.name,
            arguments.old,
            arguments.new,
            keys=arguments.key,
            sample_limit=arguments.sample_limit,
        )
    else:  # pragma: no cover - argparse enforces this
        raise AssertionError(arguments.action)
    return data, True


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        data, ok = _run_action(arguments)
        payload = {
            "schema_version": RESPONSE_SCHEMA_VERSION,
            "ok": ok,
            "action": arguments.action,
            "data": data,
        }
        if not ok:
            payload["error"] = data.get("error")
    except Exception as exc:  # noqa: BLE001 - CLI always emits one structured JSON error
        payload = {
            "schema_version": RESPONSE_SCHEMA_VERSION,
            "ok": False,
            "action": arguments.action,
            "data": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    sys.stdout.write(json.dumps(_jsonable(payload), ensure_ascii=False, allow_nan=False) + "\n")
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
