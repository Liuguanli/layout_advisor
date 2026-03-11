"""Service for dataset ingestion and summarization."""

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pyarrow import parquet as pq
from pyarrow import types as pa_types

from app.config.dataset_catalog import DATASET_CATALOG
from app.models.dataset import (
    ColumnInfo,
    ColumnProfile,
    CorrelationPair,
    CorrelationSummary,
    DatasetCatalogResponse,
    DatasetSummary,
    DistributionBucket,
    StaticDatasetItem,
)

PROFILE_SAMPLE_LIMIT = 50000
LINE_SERIES_POINTS = 16
CATEGORICAL_TOP_K = 10
MAX_CATEGORICAL_UNIQUES = 64
TOP_ASSOCIATION_LIMIT = 16

TPCH_TABLE_SCHEMAS: dict[str, list[str]] = {
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment",
    ],
    "nation": [
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment",
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
    ],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment",
    ],
    "region": [
        "r_regionkey",
        "r_name",
        "r_comment",
    ],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
}


@dataclass
class ColumnRange:
    """Global min/max range for a parquet column from metadata."""

    min_value: str | None = None
    max_value: str | None = None


class DatasetService:
    """In-memory manager for static dataset selection and summary."""

    def __init__(self) -> None:
        self._summary: DatasetSummary | None = None
        self._selected_dataset_id: str | None = None
        self._selected_dataset_path: Path | None = None
        self._profile_sample_limit = PROFILE_SAMPLE_LIMIT
        self._correlation_cache: dict[str, CorrelationSummary | None] = {}
        self._catalog: list[StaticDatasetItem] = [
            StaticDatasetItem(**item) for item in DATASET_CATALOG
        ]

    def ingest_dataset(self, filename: str, data: bytes) -> DatasetSummary:
        """Parse uploaded CSV/Parquet data and cache summary."""

        suffix = Path(filename).suffix.lower()
        file_buffer = io.BytesIO(data)

        if suffix == ".csv":
            df = pd.read_csv(file_buffer)
        elif suffix == ".tbl":
            df = _read_tbl_from_buffer(file_buffer, Path(filename).stem)
        elif suffix in {".parquet", ".pq"}:
            df = pd.read_parquet(file_buffer)
        else:
            raise ValueError("Unsupported file type. Please use CSV, TBL, or Parquet.")

        columns = [
            ColumnInfo(name=str(col), inferred_type=_infer_series_type(df[col]))
            for col, dtype in df.dtypes.items()
        ]
        profiles = _build_column_profiles(df)
        correlation_summary = _build_correlation_summary_from_frame(df)

        summary = DatasetSummary(
            dataset_id=filename,
            dataset_name=filename,
            row_count=len(df),
            profile_sample_size=len(df),
            columns=columns,
            column_profiles=profiles,
            correlation_summary=correlation_summary,
        )
        self._summary = summary
        return summary

    def list_datasets(self) -> DatasetCatalogResponse:
        """Return configured static datasets."""

        return DatasetCatalogResponse(datasets=self._catalog)

    def select_dataset(self, dataset_id: str) -> DatasetSummary:
        """Load and summarize a static dataset by configured id."""

        selected = next(
            (item for item in self._catalog if item.dataset_id == dataset_id),
            None,
        )
        if selected is None:
            raise ValueError(f"Unknown dataset_id: {dataset_id}")

        dataset_path = Path(selected.file_path)
        if not dataset_path.exists():
            raise ValueError(f"Configured dataset path does not exist: {dataset_path}")

        if dataset_path.is_dir():
            dataset_path = _resolve_dataset_file(dataset_path)

        self._selected_dataset_id = dataset_id
        self._selected_dataset_path = dataset_path
        self._profile_sample_limit = PROFILE_SAMPLE_LIMIT

        summary = _summarize_dataset_path(
            dataset_path,
            dataset_id=selected.dataset_id,
            dataset_name=selected.name,
            profile_sample_limit=self._profile_sample_limit,
        )
        self._summary = summary
        return summary

    def update_profile_sample(self, sample_size: int) -> DatasetSummary:
        """Recompute sampled column profiles for the current dataset."""

        if self._selected_dataset_path is None:
            raise ValueError("No dataset has been selected yet.")

        bounded_sample_size = max(1, sample_size)
        self._profile_sample_limit = bounded_sample_size
        summary = _summarize_dataset_path(
            self._selected_dataset_path,
            dataset_id=self._selected_dataset_id,
            dataset_name=next(
                (item.name for item in self._catalog if item.dataset_id == self._selected_dataset_id),
                self._selected_dataset_id,
            ),
            profile_sample_limit=bounded_sample_size,
        )
        self._summary = summary
        return summary

    def get_correlation_summary(self) -> CorrelationSummary | None:
        """Compute or reuse the correlation summary for the current dataset."""

        if self._selected_dataset_path is None:
            if not self._catalog:
                raise ValueError("No dataset has been selected yet.")
            self.select_dataset(self._catalog[0].dataset_id)

        if self._selected_dataset_path is None:
            raise ValueError("No dataset has been selected yet.")

        cached = self._get_cached_correlation(self._selected_dataset_path)
        if cached is not _UNCACHED:
            return cached

        suffix = self._selected_dataset_path.suffix.lower()
        if suffix in {".csv", ".tbl"}:
            df = _read_tabular_text_dataset(self._selected_dataset_path)
            correlation_summary = _build_correlation_summary_from_frame(df)
        elif suffix in {".parquet", ".pq"}:
            parquet_file = pq.ParquetFile(self._selected_dataset_path)
            correlation_summary = _build_correlation_summary_from_parquet(parquet_file)
        else:
            raise ValueError(
                f"Unsupported configured dataset type: {self._selected_dataset_path.suffix.lower()}"
            )

        self._cache_correlation(self._selected_dataset_path, correlation_summary)
        if self._summary is not None:
            self._summary.correlation_summary = correlation_summary
        return correlation_summary

    def get_summary(self) -> DatasetSummary:
        """Return latest dataset summary or raise when missing."""

        if self._summary is None:
            if not self._catalog:
                raise ValueError("No dataset has been selected yet.")
            return self.select_dataset(self._catalog[0].dataset_id)
        return self._summary

    def get_sample_frame(self, sample_size: int | None = None) -> pd.DataFrame:
        """Return a deterministic sample frame for the currently selected dataset."""

        if self._selected_dataset_path is None:
            if not self._catalog:
                raise ValueError("No dataset has been selected yet.")
            self.select_dataset(self._catalog[0].dataset_id)

        if self._selected_dataset_path is None:
            raise ValueError("No dataset has been selected yet.")

        effective_sample_size = max(1, sample_size or self._profile_sample_limit)
        suffix = self._selected_dataset_path.suffix.lower()
        if suffix in {".csv", ".tbl"}:
            return _read_tabular_text_dataset(self._selected_dataset_path, nrows=effective_sample_size)
        if suffix in {".parquet", ".pq"}:
            parquet_file = pq.ParquetFile(self._selected_dataset_path)
            return _read_parquet_sample(parquet_file, effective_sample_size)
        raise ValueError(
            f"Unsupported configured dataset type: {self._selected_dataset_path.suffix.lower()}"
        )

    def _get_cached_correlation(
        self,
        dataset_path: Path,
    ) -> CorrelationSummary | None | object:
        """Return cached correlation summary when available."""

        cache_key = str(dataset_path.resolve())
        if cache_key not in self._correlation_cache:
            return _UNCACHED
        return self._correlation_cache[cache_key]

    def _cache_correlation(
        self,
        dataset_path: Path,
        correlation_summary: CorrelationSummary | None,
    ) -> None:
        """Persist correlation summary for reuse across sample-size updates."""

        self._correlation_cache[str(dataset_path.resolve())] = correlation_summary


def _infer_dtype(dtype: pd.api.extensions.ExtensionDtype) -> str:
    """Map pandas dtype to simplified semantic type."""

    if pd.api.types.is_integer_dtype(dtype):
        return "integer"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    return "string"


def _infer_arrow_type(dtype: object) -> str:
    """Map pyarrow dtype to simplified semantic type."""

    if pa_types.is_integer(dtype):
        return "integer"
    if pa_types.is_floating(dtype) or pa_types.is_decimal(dtype):
        return "float"
    if pa_types.is_boolean(dtype):
        return "boolean"
    if pa_types.is_date(dtype) or pa_types.is_timestamp(dtype):
        return "datetime"
    return "string"


_UNCACHED: Any = object()


def _summarize_dataset_path(
    dataset_path: Path,
    dataset_id: str | None = None,
    dataset_name: str | None = None,
    profile_sample_limit: int = PROFILE_SAMPLE_LIMIT,
) -> DatasetSummary:
    """Build dataset summary without unnecessary full-file loading."""

    suffix = dataset_path.suffix.lower()
    if suffix in {".csv", ".tbl"}:
        row_count = _count_text_rows(dataset_path, suffix)
        df = _read_tabular_text_dataset(dataset_path, nrows=profile_sample_limit)
        columns = [
            ColumnInfo(name=str(col), inferred_type=_infer_series_type(df[col]))
            for col, dtype in df.dtypes.items()
        ]
        sample_df = df if len(df) <= profile_sample_limit else _sample_frame(df, profile_sample_limit)
        return DatasetSummary(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            row_count=row_count,
            profile_sample_size=len(sample_df),
            columns=columns,
            column_profiles=_build_column_profiles(sample_df),
            correlation_summary=None,
        )

    if suffix in {".parquet", ".pq"}:
        parquet_file = pq.ParquetFile(dataset_path)
        schema = parquet_file.schema_arrow
        columns = [
            ColumnInfo(name=field.name, inferred_type=_infer_arrow_type(field.type))
            for field in schema
        ]
        sample_df = _read_parquet_sample(parquet_file, profile_sample_limit)
        column_ranges = _read_parquet_column_ranges(parquet_file)
        return DatasetSummary(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            row_count=parquet_file.metadata.num_rows,
            profile_sample_size=len(sample_df),
            columns=columns,
            column_profiles=_build_column_profiles(sample_df, column_ranges),
            correlation_summary=None,
        )

    raise ValueError(f"Unsupported configured dataset type: {suffix}")


def _resolve_dataset_file(directory: Path) -> Path:
    """Resolve first supported dataset file from a directory path."""

    candidates = sorted(directory.glob("*.csv"))
    candidates += sorted(directory.glob("*.tbl"))
    candidates += sorted(directory.glob("*.parquet"))
    candidates += sorted(directory.glob("*.pq"))
    if not candidates:
        raise ValueError(
            f"No CSV/TBL/Parquet files found in configured dataset directory: {directory}"
        )
    return candidates[0]


def _count_text_rows(dataset_path: Path, suffix: str) -> int:
    """Count logical rows for CSV/TBL files without loading them into pandas."""

    with dataset_path.open("r", encoding="utf-8", errors="ignore") as handle:
        line_count = sum(1 for _ in handle)

    if suffix == ".csv":
        return max(line_count - 1, 0)
    return line_count


def _read_tabular_text_dataset(
    dataset_path: Path,
    nrows: int | None = None,
) -> pd.DataFrame:
    """Read CSV or TBL text datasets with format-specific handling."""

    suffix = dataset_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(dataset_path, nrows=nrows)
    if suffix == ".tbl":
        return _read_tbl_file(dataset_path, nrows=nrows)
    raise ValueError(f"Unsupported tabular text dataset type: {suffix}")


def _read_tbl_file(dataset_path: Path, nrows: int | None = None) -> pd.DataFrame:
    """Read a .tbl file as pipe-delimited text, with TPC-H schema support."""

    with dataset_path.open("rb") as handle:
        return _read_tbl_from_buffer(handle, dataset_path.stem, nrows=nrows)


def _read_tbl_from_buffer(
    buffer: io.BytesIO | Any,
    dataset_stem: str,
    nrows: int | None = None,
) -> pd.DataFrame:
    """Read a .tbl buffer using pipe delimiters and normalize trailing separators."""

    frame = pd.read_csv(buffer, sep="|", header=None, nrows=nrows)
    if len(frame.columns) > 0 and frame.iloc[:, -1].isna().all():
        frame = frame.iloc[:, :-1].copy()

    schema = TPCH_TABLE_SCHEMAS.get(dataset_stem.lower())
    if schema and len(schema) == len(frame.columns):
        frame.columns = schema
    else:
        frame.columns = [f"col_{index + 1}" for index in range(len(frame.columns))]
    return frame


def _sample_frame(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    """Limit a dataframe to a bounded sample for profile computation."""

    if len(df) <= max_rows:
        return df
    return df.sample(n=max_rows, random_state=0)


def _read_parquet_sample(parquet_file: pq.ParquetFile, max_rows: int) -> pd.DataFrame:
    """Read a bounded row sample from a parquet file quickly.

    We intentionally avoid full-file random sampling here because dataset load should
    stay responsive. The sample is built from a small set of evenly spaced row groups.
    """

    total_rows = parquet_file.metadata.num_rows
    if total_rows == 0:
        return pd.DataFrame(columns=[field.name for field in parquet_file.schema_arrow])

    frames: list[pd.DataFrame] = []
    row_group_count = parquet_file.metadata.num_row_groups
    if row_group_count == 0:
        return pd.DataFrame(columns=[field.name for field in parquet_file.schema_arrow])

    if total_rows <= max_rows:
        selected_row_groups = list(range(row_group_count))
    else:
        approx_rows_per_group = max(total_rows / row_group_count, 1)
        target_group_count = min(
            row_group_count,
            max(1, int(np.ceil(max_rows / approx_rows_per_group))),
        )
        selected_row_groups = np.linspace(
            0,
            row_group_count - 1,
            num=target_group_count,
            dtype=int,
        ).tolist()

    remaining = max_rows
    for row_group_index in selected_row_groups:
        table = parquet_file.read_row_group(row_group_index)
        frame = table.to_pandas()
        if len(frame) > remaining:
            frame = frame.iloc[:remaining].copy()
        frames.append(frame)
        remaining -= len(frame)
        if remaining <= 0:
            break

    if not frames:
        return pd.DataFrame(columns=[field.name for field in parquet_file.schema_arrow])
    return pd.concat(frames, ignore_index=True)


def _build_column_profiles(
    df: pd.DataFrame,
    global_ranges: dict[str, ColumnRange] | None = None,
) -> list[ColumnProfile]:
    """Build sampled distributions for each dataframe column."""

    profiles: list[ColumnProfile] = []
    for column in df.columns:
        series = df[column]
        inferred_type = _infer_series_type(series)
        non_null = series.dropna()
        min_value, max_value = _profile_range_values(non_null, inferred_type)
        if global_ranges and str(column) in global_ranges:
            range_info = global_ranges[str(column)]
            min_value = range_info.min_value or min_value
            max_value = range_info.max_value or max_value
        profile = ColumnProfile(
            name=str(column),
            inferred_type=inferred_type,
            sample_size=len(series),
            null_count=int(series.isna().sum()),
            distinct_count=int(non_null.nunique()),
            min_value=min_value,
            max_value=max_value,
            distribution_kind=_distribution_kind(inferred_type, non_null),
            distribution=_build_distribution(non_null, inferred_type),
        )
        profiles.append(profile)
    return profiles


def _build_correlation_summary_from_frame(df: pd.DataFrame) -> CorrelationSummary | None:
    """Compute an all-column association matrix from an in-memory dataframe."""

    columns = [str(column) for column in df.columns]
    if not columns:
        return None

    column_types = {column: _infer_series_type(df[column]) for column in df.columns}
    column_kinds = _classify_frame_column_kinds(df, column_types)

    matrix: list[list[float | None]] = []
    top_pairs: list[CorrelationPair] = []

    for row_index, column_a in enumerate(columns):
        row: list[float | None] = []
        for col_index, column_b in enumerate(columns):
            if row_index == col_index:
                row.append(1.0)
                continue
            if col_index < row_index:
                row.append(matrix[col_index][row_index])
                continue

            value, observations = _association_from_series(
                df[column_a],
                column_types[column_a],
                column_kinds[column_a],
                df[column_b],
                column_types[column_b],
                column_kinds[column_b],
            )
            rounded = None if value is None else round(value, 4)
            row.append(rounded)

            if rounded is not None:
                top_pairs.append(
                    CorrelationPair(
                        column_a=column_a,
                        column_b=column_b,
                        correlation=rounded,
                        observation_count=observations,
                    )
                )
        matrix.append(row)

    top_pairs.sort(key=lambda pair: pair.correlation, reverse=True)
    return CorrelationSummary(
        method="mixed_association",
        mode="exact_full_scan",
        columns=columns,
        column_kinds=column_kinds,
        matrix=matrix,
        top_pairs=top_pairs[:TOP_ASSOCIATION_LIMIT],
    )


def _build_correlation_summary_from_parquet(
    parquet_file: pq.ParquetFile,
) -> CorrelationSummary | None:
    """Compute an all-column association matrix with full parquet scans."""

    columns = [field.name for field in parquet_file.schema_arrow]
    if not columns:
        return None

    column_types = {
        field.name: _infer_arrow_type(field.type) for field in parquet_file.schema_arrow
    }
    column_kinds = _detect_parquet_column_kinds(parquet_file, column_types)

    supported_columns = [
        column for column in columns if column_kinds[column] != "unsupported_text"
    ]
    aggregators = _build_pair_aggregators(columns, column_kinds)

    if supported_columns:
        for batch in parquet_file.iter_batches(columns=supported_columns, batch_size=8192):
            frame = batch.to_pandas()
            ordered_arrays = {
                column: _series_to_ordered_array(frame[column], column_types[column])
                for column in supported_columns
                if column_kinds[column] == "ordered"
            }
            categorical_arrays = {
                column: _series_to_categorical_array(frame[column])
                for column in supported_columns
                if column_kinds[column] == "categorical"
            }

            for (column_a, column_b), aggregator in aggregators.items():
                pair_kind = aggregator["pair_kind"]
                if pair_kind == "ordered_ordered":
                    _update_ordered_ordered_aggregator(
                        aggregator,
                        ordered_arrays[column_a],
                        ordered_arrays[column_b],
                    )
                elif pair_kind == "categorical_categorical":
                    _update_categorical_categorical_aggregator(
                        aggregator,
                        categorical_arrays[column_a],
                        categorical_arrays[column_b],
                    )
                elif pair_kind == "categorical_ordered":
                    categorical_column = aggregator["categorical_column"]
                    ordered_column = aggregator["ordered_column"]
                    _update_categorical_ordered_aggregator(
                        aggregator,
                        categorical_arrays[categorical_column],
                        ordered_arrays[ordered_column],
                    )

    matrix, top_pairs = _finalize_association_summary(columns, column_kinds, aggregators)
    return CorrelationSummary(
        method="mixed_association",
        mode="exact_full_scan",
        columns=columns,
        column_kinds=column_kinds,
        matrix=matrix,
        top_pairs=top_pairs[:TOP_ASSOCIATION_LIMIT],
    )


def _classify_frame_column_kinds(
    df: pd.DataFrame,
    column_types: dict[str, str],
) -> dict[str, str]:
    """Classify dataframe columns into ordered, categorical, or unsupported text."""

    kinds: dict[str, str] = {}
    for column in df.columns:
        inferred_type = column_types[str(column)]
        if inferred_type in {"integer", "float", "datetime"}:
            kinds[str(column)] = "ordered"
        elif inferred_type == "boolean":
            kinds[str(column)] = "categorical"
        else:
            distinct_count = int(df[column].dropna().nunique())
            kinds[str(column)] = (
                "categorical"
                if distinct_count <= MAX_CATEGORICAL_UNIQUES
                else "unsupported_text"
            )
    return kinds


def _detect_parquet_column_kinds(
    parquet_file: pq.ParquetFile,
    column_types: dict[str, str],
) -> dict[str, str]:
    """Classify parquet columns using exact scans for low-cardinality string columns."""

    kinds: dict[str, str] = {}
    pending_string_columns: dict[str, set[str]] = {}

    for column, inferred_type in column_types.items():
        if inferred_type in {"integer", "float", "datetime"}:
            kinds[column] = "ordered"
        elif inferred_type == "boolean":
            kinds[column] = "categorical"
        else:
            pending_string_columns[column] = set()

    if pending_string_columns:
        for batch in parquet_file.iter_batches(
            columns=list(pending_string_columns.keys()),
            batch_size=8192,
        ):
            frame = batch.to_pandas()
            finished_columns: list[str] = []
            for column, distinct_values in pending_string_columns.items():
                values = frame[column].dropna()
                if values.empty:
                    continue
                distinct_values.update(values.astype(str).unique().tolist())
                if len(distinct_values) > MAX_CATEGORICAL_UNIQUES:
                    kinds[column] = "unsupported_text"
                    finished_columns.append(column)

            for column in finished_columns:
                pending_string_columns.pop(column, None)

            if not pending_string_columns:
                break

    for column, distinct_values in pending_string_columns.items():
        kinds[column] = (
            "categorical"
            if len(distinct_values) <= MAX_CATEGORICAL_UNIQUES
            else "unsupported_text"
        )

    return kinds


def _association_from_series(
    series_a: pd.Series,
    type_a: str,
    kind_a: str,
    series_b: pd.Series,
    type_b: str,
    kind_b: str,
) -> tuple[float | None, int]:
    """Compute exact pairwise association between two in-memory columns."""

    if "unsupported_text" in {kind_a, kind_b}:
        return None, 0

    if kind_a == "ordered" and kind_b == "ordered":
        return _ordered_ordered_association(
            _series_to_ordered_array(series_a, type_a),
            _series_to_ordered_array(series_b, type_b),
        )

    if kind_a == "categorical" and kind_b == "categorical":
        return _categorical_categorical_association(
            _series_to_categorical_array(series_a),
            _series_to_categorical_array(series_b),
        )

    if kind_a == "categorical":
        return _categorical_ordered_association(
            _series_to_categorical_array(series_a),
            _series_to_ordered_array(series_b, type_b),
        )

    return _categorical_ordered_association(
        _series_to_categorical_array(series_b),
        _series_to_ordered_array(series_a, type_a),
    )


def _series_to_ordered_array(series: pd.Series, inferred_type: str) -> np.ndarray:
    """Convert ordered columns into numeric vectors for association analysis."""

    if inferred_type == "datetime":
        datetimes = pd.to_datetime(series, errors="coerce")
        values = np.full(len(datetimes), np.nan, dtype=np.float64)
        valid_mask = datetimes.notna().to_numpy()
        if valid_mask.any():
            values[valid_mask] = (
                datetimes[valid_mask].astype("int64").to_numpy(dtype=np.float64)
                / 86_400_000_000_000
            )
        return values

    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.to_numpy(dtype=np.float64, na_value=np.nan)


def _series_to_categorical_array(series: pd.Series) -> np.ndarray:
    """Convert categorical columns to object arrays while preserving missing values."""

    values = series.astype("object").to_numpy(copy=True)
    return np.array([None if pd.isna(value) else str(value) for value in values], dtype=object)


def _ordered_ordered_association(
    values_a: np.ndarray,
    values_b: np.ndarray,
) -> tuple[float | None, int]:
    """Compute absolute Pearson association for two ordered vectors."""

    mask = np.isfinite(values_a) & np.isfinite(values_b)
    observation_count = int(mask.sum())
    if observation_count < 2:
        return None, observation_count

    x = values_a[mask]
    y = values_b[mask]
    var_x = float(np.var(x))
    var_y = float(np.var(y))
    if var_x == 0 or var_y == 0:
        return None, observation_count

    correlation = np.corrcoef(x, y)[0, 1]
    return float(abs(correlation)), observation_count


def _categorical_categorical_association(
    values_a: np.ndarray,
    values_b: np.ndarray,
) -> tuple[float | None, int]:
    """Compute bias-corrected Cramer's V for two categorical vectors."""

    mask = np.array(
        [value_a is not None and value_b is not None for value_a, value_b in zip(values_a, values_b)],
        dtype=bool,
    )
    observation_count = int(mask.sum())
    if observation_count < 2:
        return None, observation_count

    frame = pd.DataFrame({"a": values_a[mask], "b": values_b[mask]})
    contingency = pd.crosstab(frame["a"], frame["b"])
    if contingency.empty:
        return None, observation_count

    observed = contingency.to_numpy(dtype=np.float64)
    row_totals = observed.sum(axis=1, keepdims=True)
    col_totals = observed.sum(axis=0, keepdims=True)
    expected = row_totals @ col_totals / observed.sum()
    if np.any(expected == 0):
        return None, observation_count

    chi_square = float(((observed - expected) ** 2 / expected).sum())
    return _cramers_v_from_components(
        chi_square,
        observation_count,
        observed.shape[0],
        observed.shape[1],
    ), observation_count


def _categorical_ordered_association(
    categories: np.ndarray,
    values: np.ndarray,
) -> tuple[float | None, int]:
    """Compute correlation ratio eta for categorical-ordered pairs."""

    mask = np.array(
        [category is not None and np.isfinite(value) for category, value in zip(categories, values)],
        dtype=bool,
    )
    observation_count = int(mask.sum())
    if observation_count < 2:
        return None, observation_count

    frame = pd.DataFrame({"category": categories[mask], "value": values[mask]})
    grouped = frame.groupby("category", observed=True)["value"].agg(["count", "sum"])
    total_sum = float(frame["value"].sum())
    total_sum_sq = float((frame["value"] ** 2).sum())
    total_ss = total_sum_sq - (total_sum * total_sum / observation_count)
    if total_ss <= 0:
        return None, observation_count

    between_ss = float(((grouped["sum"] ** 2) / grouped["count"]).sum()) - (
        total_sum * total_sum / observation_count
    )
    eta_squared = max(min(between_ss / total_ss, 1.0), 0.0)
    return float(np.sqrt(eta_squared)), observation_count


def _cramers_v_from_components(
    chi_square: float,
    observation_count: int,
    row_count: int,
    column_count: int,
) -> float | None:
    """Compute bias-corrected Cramer's V from contingency table components."""

    if observation_count <= 1 or row_count <= 1 or column_count <= 1:
        return None

    phi_squared = chi_square / observation_count
    phi_squared_corrected = max(
        0.0,
        phi_squared - ((column_count - 1) * (row_count - 1)) / (observation_count - 1),
    )
    row_count_corrected = row_count - ((row_count - 1) ** 2) / (observation_count - 1)
    column_count_corrected = column_count - ((column_count - 1) ** 2) / (observation_count - 1)
    denominator = min(column_count_corrected - 1, row_count_corrected - 1)
    if denominator <= 0:
        return None
    return float(np.sqrt(phi_squared_corrected / denominator))


def _build_pair_aggregators(
    columns: list[str],
    column_kinds: dict[str, str],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Create streaming aggregators for supported parquet column pairs."""

    aggregators: dict[tuple[str, str], dict[str, Any]] = {}
    for row_index, column_a in enumerate(columns):
        for col_index in range(row_index + 1, len(columns)):
            column_b = columns[col_index]
            kind_a = column_kinds[column_a]
            kind_b = column_kinds[column_b]
            if "unsupported_text" in {kind_a, kind_b}:
                continue

            if kind_a == "ordered" and kind_b == "ordered":
                aggregators[(column_a, column_b)] = {
                    "pair_kind": "ordered_ordered",
                    "n": 0,
                    "sum_x": 0.0,
                    "sum_y": 0.0,
                    "sum_x2": 0.0,
                    "sum_y2": 0.0,
                    "sum_xy": 0.0,
                }
            elif kind_a == "categorical" and kind_b == "categorical":
                aggregators[(column_a, column_b)] = {
                    "pair_kind": "categorical_categorical",
                    "n": 0,
                    "pair_counts": {},
                    "a_levels": set(),
                    "b_levels": set(),
                }
            else:
                categorical_column = column_a if kind_a == "categorical" else column_b
                ordered_column = column_b if kind_a == "categorical" else column_a
                aggregators[(column_a, column_b)] = {
                    "pair_kind": "categorical_ordered",
                    "categorical_column": categorical_column,
                    "ordered_column": ordered_column,
                    "n": 0,
                    "sum_y": 0.0,
                    "sum_y2": 0.0,
                    "groups": {},
                }

    return aggregators


def _update_ordered_ordered_aggregator(
    aggregator: dict[str, Any],
    values_a: np.ndarray,
    values_b: np.ndarray,
) -> None:
    """Update exact Pearson components for an ordered-ordered pair."""

    mask = np.isfinite(values_a) & np.isfinite(values_b)
    if not mask.any():
        return

    x = values_a[mask]
    y = values_b[mask]
    aggregator["n"] += len(x)
    aggregator["sum_x"] += float(x.sum())
    aggregator["sum_y"] += float(y.sum())
    aggregator["sum_x2"] += float((x * x).sum())
    aggregator["sum_y2"] += float((y * y).sum())
    aggregator["sum_xy"] += float((x * y).sum())


def _update_categorical_categorical_aggregator(
    aggregator: dict[str, Any],
    values_a: np.ndarray,
    values_b: np.ndarray,
) -> None:
    """Update contingency counts for a categorical-categorical pair."""

    mask = np.array(
        [value_a is not None and value_b is not None for value_a, value_b in zip(values_a, values_b)],
        dtype=bool,
    )
    if not mask.any():
        return

    frame = pd.DataFrame({"a": values_a[mask], "b": values_b[mask]})
    counts = frame.value_counts().items()
    aggregator["n"] += int(mask.sum())
    for (value_a, value_b), count in counts:
        aggregator["pair_counts"][(str(value_a), str(value_b))] = (
            aggregator["pair_counts"].get((str(value_a), str(value_b)), 0) + int(count)
        )
        aggregator["a_levels"].add(str(value_a))
        aggregator["b_levels"].add(str(value_b))


def _update_categorical_ordered_aggregator(
    aggregator: dict[str, Any],
    categories: np.ndarray,
    values: np.ndarray,
) -> None:
    """Update eta components for a categorical-ordered pair."""

    mask = np.array(
        [category is not None and np.isfinite(value) for category, value in zip(categories, values)],
        dtype=bool,
    )
    if not mask.any():
        return

    frame = pd.DataFrame({"category": categories[mask], "value": values[mask]})
    grouped = frame.groupby("category", observed=True)["value"].agg(["count", "sum"])
    aggregator["n"] += int(mask.sum())
    aggregator["sum_y"] += float(frame["value"].sum())
    aggregator["sum_y2"] += float((frame["value"] ** 2).sum())

    for category, row in grouped.iterrows():
        existing_count, existing_sum = aggregator["groups"].get(str(category), (0, 0.0))
        aggregator["groups"][str(category)] = (
            existing_count + int(row["count"]),
            existing_sum + float(row["sum"]),
        )


def _finalize_association_summary(
    columns: list[str],
    column_kinds: dict[str, str],
    aggregators: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[list[float | None]], list[CorrelationPair]]:
    """Build a dense association matrix and strongest-pair list."""

    matrix: list[list[float | None]] = []
    top_pairs: list[CorrelationPair] = []

    for row_index, column_a in enumerate(columns):
        row: list[float | None] = []
        for col_index, column_b in enumerate(columns):
            if row_index == col_index:
                row.append(1.0)
                continue

            if col_index < row_index:
                row.append(matrix[col_index][row_index])
                continue

            aggregator = aggregators.get((column_a, column_b))
            if aggregator is None:
                row.append(None)
                continue

            pair_kind = aggregator["pair_kind"]
            observations = int(aggregator["n"])
            value: float | None = None

            if pair_kind == "ordered_ordered":
                value = _finalize_ordered_ordered_aggregator(aggregator)
            elif pair_kind == "categorical_categorical":
                value = _finalize_categorical_categorical_aggregator(aggregator)
            elif pair_kind == "categorical_ordered":
                value = _finalize_categorical_ordered_aggregator(aggregator)

            rounded = None if value is None else round(value, 4)
            row.append(rounded)

            if rounded is not None:
                top_pairs.append(
                    CorrelationPair(
                        column_a=column_a,
                        column_b=column_b,
                        correlation=rounded,
                        observation_count=observations,
                    )
                )

        matrix.append(row)

    top_pairs.sort(key=lambda pair: pair.correlation, reverse=True)
    return matrix, top_pairs


def _finalize_ordered_ordered_aggregator(
    aggregator: dict[str, Any],
) -> float | None:
    """Finalize absolute Pearson association from streaming components."""

    observation_count = int(aggregator["n"])
    if observation_count < 2:
        return None

    sum_x = float(aggregator["sum_x"])
    sum_y = float(aggregator["sum_y"])
    sum_x2 = float(aggregator["sum_x2"])
    sum_y2 = float(aggregator["sum_y2"])
    sum_xy = float(aggregator["sum_xy"])

    cov_xy = sum_xy - (sum_x * sum_y / observation_count)
    var_x = sum_x2 - (sum_x * sum_x / observation_count)
    var_y = sum_y2 - (sum_y * sum_y / observation_count)
    if var_x <= 0 or var_y <= 0:
        return None

    correlation = cov_xy / float(np.sqrt(var_x * var_y))
    return float(abs(max(min(correlation, 1.0), -1.0)))


def _finalize_categorical_categorical_aggregator(
    aggregator: dict[str, Any],
) -> float | None:
    """Finalize bias-corrected Cramer's V from streaming contingency counts."""

    observation_count = int(aggregator["n"])
    pair_counts = aggregator["pair_counts"]
    if observation_count < 2 or not pair_counts:
        return None

    row_labels = sorted(aggregator["a_levels"])
    col_labels = sorted(aggregator["b_levels"])
    contingency = np.zeros((len(row_labels), len(col_labels)), dtype=np.float64)
    row_index = {label: index for index, label in enumerate(row_labels)}
    col_index = {label: index for index, label in enumerate(col_labels)}

    for (value_a, value_b), count in pair_counts.items():
        contingency[row_index[value_a], col_index[value_b]] = count

    row_totals = contingency.sum(axis=1, keepdims=True)
    col_totals = contingency.sum(axis=0, keepdims=True)
    expected = row_totals @ col_totals / contingency.sum()
    if np.any(expected == 0):
        return None

    chi_square = float(((contingency - expected) ** 2 / expected).sum())
    return _cramers_v_from_components(
        chi_square,
        observation_count,
        contingency.shape[0],
        contingency.shape[1],
    )


def _finalize_categorical_ordered_aggregator(
    aggregator: dict[str, Any],
) -> float | None:
    """Finalize correlation ratio eta from streaming grouped sums."""

    observation_count = int(aggregator["n"])
    if observation_count < 2:
        return None

    total_sum = float(aggregator["sum_y"])
    total_sum_sq = float(aggregator["sum_y2"])
    total_ss = total_sum_sq - (total_sum * total_sum / observation_count)
    if total_ss <= 0:
        return None

    between_ss = sum(
        (group_sum * group_sum) / group_count
        for group_count, group_sum in aggregator["groups"].values()
        if group_count > 0
    ) - (total_sum * total_sum / observation_count)

    eta_squared = max(min(float(between_ss) / total_ss, 1.0), 0.0)
    return float(np.sqrt(eta_squared))


def _distribution_kind(inferred_type: str, non_null: pd.Series) -> str:
    """Return the chart family to render for a column."""

    if non_null.empty:
        return "empty"
    if inferred_type == "datetime":
        return "datetime_line"
    if inferred_type in {"integer", "float"}:
        return "line"
    return "categorical"


def _build_distribution(non_null: pd.Series, inferred_type: str) -> list[DistributionBucket]:
    """Build line or categorical distributions from a sampled series."""

    if non_null.empty:
        return []

    if inferred_type == "datetime":
        return _build_datetime_line_series(pd.to_datetime(non_null, errors="coerce").dropna())
    if inferred_type in {"integer", "float"}:
        numeric = pd.to_numeric(non_null, errors="coerce").dropna()
        return _build_numeric_line_series(numeric, inferred_type)
    return _build_categorical_histogram(non_null.astype(str))


def _build_numeric_line_series(
    series: pd.Series,
    inferred_type: str,
) -> list[DistributionBucket]:
    """Build a line-friendly numeric distribution series."""

    if series.empty:
        return []

    unique_count = int(series.nunique())
    if unique_count <= 1:
        return [
            DistributionBucket(label=_format_value(series.iloc[0]) or "value", count=len(series))
        ]

    bucket_count = min(LINE_SERIES_POINTS, unique_count)
    binned = pd.cut(series, bins=bucket_count, include_lowest=True, duplicates="drop")
    counts = binned.value_counts(sort=False)
    return [
        DistributionBucket(
            label=_format_numeric_midpoint(interval.left, interval.right, inferred_type),
            count=int(count),
        )
        for interval, count in counts.items()
    ]


def _build_datetime_line_series(series: pd.Series) -> list[DistributionBucket]:
    """Build a line-friendly datetime distribution series."""

    if series.empty:
        return []

    min_ts = series.min()
    max_ts = series.max()
    span_days = max((max_ts - min_ts).days, 0)
    freq = "M" if span_days <= 365 * 2 else "Q"
    counts = series.dt.to_period(freq).value_counts().sort_index()
    return [
        DistributionBucket(
            label=_format_period_label(period),
            count=int(count),
        )
        for period, count in counts.items()
    ]


def _build_categorical_histogram(series: pd.Series) -> list[DistributionBucket]:
    """Build top-k categorical frequency buckets."""

    counts = series.value_counts().head(CATEGORICAL_TOP_K)
    return [
        DistributionBucket(label=str(label), count=int(count))
        for label, count in counts.items()
    ]


def _format_numeric_midpoint(left: float, right: float, inferred_type: str) -> str:
    """Format numeric bin midpoint for a line chart x-axis."""

    midpoint = (left + right) / 2
    if inferred_type == "integer":
        return str(int(round(midpoint)))
    return _format_number(midpoint)


def _format_value(value: object) -> str | None:
    """Format scalar values for summary fields."""

    if value is None or pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return str(value)
    if isinstance(value, float):
        return _format_number(value)
    return str(value)


def _format_number(value: float) -> str:
    """Format numeric values compactly for display."""

    abs_value = abs(value)
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.1f}K"
    if abs_value >= 100:
        return f"{value:.0f}"
    if abs_value >= 1:
        return f"{value:.2f}"
    return f"{value:.4f}"


def _format_period_label(period: pd.Period) -> str:
    """Format monthly/quarterly periods for a line chart x-axis."""

    if period.freqstr.startswith("Q"):
        return f"{period.year}-Q{period.quarter}"
    return period.strftime("%Y-%m")


def _profile_range_values(non_null: pd.Series, inferred_type: str) -> tuple[str | None, str | None]:
    """Return min/max only for ordered column types."""

    if non_null.empty or inferred_type not in {"integer", "float", "datetime"}:
        return None, None
    return _format_value(non_null.min()), _format_value(non_null.max())


def _infer_series_type(series: pd.Series) -> str:
    """Infer semantic type from a pandas series, including object-backed dates."""

    inferred_type = _infer_dtype(series.dtype)
    if inferred_type != "string":
        return inferred_type

    non_null = series.dropna()
    if non_null.empty:
        return inferred_type

    sample = non_null.head(min(200, len(non_null))).astype(str).str.strip()
    if not _sample_looks_like_datetime(sample):
        return inferred_type

    parsed = pd.to_datetime(sample, errors="coerce", format="mixed")
    if len(parsed) > 0 and parsed.notna().mean() >= 0.9:
        return "datetime"
    return inferred_type


def _sample_looks_like_datetime(sample: pd.Series) -> bool:
    """Return whether a string sample looks date-like enough to justify parsing."""

    if sample.empty:
        return False

    normalized = sample.str.strip()
    if normalized.empty:
        return False

    # Fast reject for free-text/string identifier columns. We only try parsing when
    # most values look structurally date-like.
    iso_like = normalized.str.fullmatch(
        r"\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?"
    )
    compact_like = normalized.str.fullmatch(r"\d{8}")
    month_name_like = normalized.str.fullmatch(
        r"[A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}"
    )
    date_like_ratio = (iso_like | compact_like | month_name_like).mean()
    return bool(date_like_ratio >= 0.8)


def _read_parquet_column_ranges(parquet_file: pq.ParquetFile) -> dict[str, ColumnRange]:
    """Read global column min/max from parquet row-group statistics when available."""

    ranges: dict[str, ColumnRange] = {}
    metadata = parquet_file.metadata
    for column_index, field in enumerate(parquet_file.schema_arrow):
        field_type = _infer_arrow_type(field.type)
        if field_type not in {"integer", "float", "datetime"}:
            continue

        min_raw = None
        max_raw = None
        for row_group_index in range(metadata.num_row_groups):
            column_meta = metadata.row_group(row_group_index).column(column_index)
            stats = column_meta.statistics
            if stats is None or not stats.has_min_max:
                continue
            if min_raw is None or stats.min < min_raw:
                min_raw = stats.min
            if max_raw is None or stats.max > max_raw:
                max_raw = stats.max

        ranges[field.name] = ColumnRange(
            min_value=_format_value(_normalize_stat_value(min_raw)),
            max_value=_format_value(_normalize_stat_value(max_raw)),
        )
    return ranges


def _normalize_stat_value(value: object) -> object:
    """Normalize parquet stats values into pandas-friendly scalars."""

    if value is None:
        return None
    try:
        return pd.Timestamp(value).to_pydatetime() if isinstance(value, pd.Timestamp) else value
    except Exception:
        return value
