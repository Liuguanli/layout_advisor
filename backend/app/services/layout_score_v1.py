"""score_v1 helpers for sample-based layout sorting and pruning simulation."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from app.models.layout import QueryEstimate
from app.utils.sql_parser import PredicateRecord

if TYPE_CHECKING:
    from app.services.layout_service import BaseQueryAccessEstimator, QueryEvaluationContext


SCORE_V1_ALGORITHM_NAME = "score_v1_sample_pruning"
UINT64_SIGN_MASK = np.uint64(1 << 63)
UINT64_ALL_ONES = np.uint64((1 << 64) - 1)


@dataclass(frozen=True)
class EncodedRange:
    """Encoded predicate range on one column."""

    lower: int | None = None
    upper: int | None = None
    lower_inclusive: bool = True
    upper_inclusive: bool = True


@dataclass(frozen=True)
class RowGroupStat:
    """Min/max summary for one simulated parquet row group."""

    row_count: int
    column_bounds: dict[str, tuple[int, int] | None]


class SamplePruningScoreV1Estimator:
    """Actual sample-based estimator for `no layout` and `linear`.

    It performs:
    - 64-bit encoding for selected layout columns
    - real sample sorting for `linear`
    - row-group min/max simulation
    - query predicate to encoded-range conversion
    - pruning-based read estimation
    """

    algorithm_name = SCORE_V1_ALGORITHM_NAME
    supported_layouts = {"no layout", "linear"}

    def estimate_queries(
        self,
        layout_type: str,
        candidate,
        context: "QueryEvaluationContext",
    ) -> list[QueryEstimate]:
        if layout_type not in self.supported_layouts:
            raise ValueError(f"Unsupported score_v1 layout type: {layout_type}")

        sample_frame = context.sample_frame.copy()
        if sample_frame.empty:
            return [
                QueryEstimate(
                    query_id=f"q{index + 1:04d}",
                    predicate_columns=sorted({predicate.column for predicate in predicates}),
                    estimated_records_read=0,
                    estimated_bytes_read=0,
                    estimated_row_groups_read=0,
                    benefit_vs_baseline=0.0,
                )
                for index, predicates in enumerate(context.query_predicates)
            ]

        filter_columns = sorted(
            {
                predicate.column
                for predicates in context.query_predicates
                for predicate in predicates
                if predicate.column in context.column_types
            }
        )
        rows_per_group_sample = estimate_rows_per_group_sample(
            sample_frame=sample_frame,
            dataset_row_count=context.dataset_row_count,
            sample_ratio=context.sample_ratio,
            default_row_group_bytes=context.default_row_group_bytes,
        )
        partition_infos = build_partition_infos(
            sample_frame=sample_frame,
            partition_strategy=context.partition_strategy,
            partition_columns=context.partition_columns,
            layout_type=layout_type,
            layout_columns=candidate.columns,
            column_types=context.column_types,
            filter_columns=filter_columns,
            rows_per_group_sample=rows_per_group_sample,
        )

        avg_record_bytes = max(context.total_bytes // max(context.dataset_row_count, 1), 1)
        estimates: list[QueryEstimate] = []
        for index, predicates in enumerate(context.query_predicates, start=1):
            query_ranges = query_to_column_ranges(predicates, context.column_types)
            touched_partitions = touched_partition_infos(
                partition_infos=partition_infos,
                predicates=predicates,
                column_types=context.column_types,
            )
            groups_read = 0
            sample_rows_read = 0
            for partition_info in touched_partitions:
                if not query_ranges:
                    groups_read += len(partition_info.row_group_stats)
                    sample_rows_read += partition_info.row_count
                    continue
                for row_group in partition_info.row_group_stats:
                    if row_group_matches_query(row_group, query_ranges):
                        groups_read += 1
                        sample_rows_read += row_group.row_count

            estimated_records_read = scale_sample_rows_to_full(
                sample_rows_read=sample_rows_read,
                sample_ratio=context.sample_ratio,
                full_row_count=context.dataset_row_count,
            )
            estimated_row_groups_read = min(context.total_row_groups, groups_read)
            estimated_bytes_read = min(
                context.total_bytes,
                estimated_records_read * avg_record_bytes,
            )
            estimates.append(
                QueryEstimate(
                    query_id=f"q{index:04d}",
                    predicate_columns=sorted({predicate.column for predicate in predicates}),
                    estimated_records_read=estimated_records_read,
                    estimated_bytes_read=estimated_bytes_read,
                    estimated_row_groups_read=estimated_row_groups_read,
                    benefit_vs_baseline=0.0,
                )
            )

        return estimates


@dataclass(frozen=True)
class PartitionInfo:
    """One partition slice with layout-specific row-group stats."""

    key: tuple[object, ...]
    partition_columns: tuple[str, ...]
    frame: pd.DataFrame
    row_count: int
    row_group_stats: list[RowGroupStat]


def build_partition_infos(
    *,
    sample_frame: pd.DataFrame,
    partition_strategy: str,
    partition_columns: list[str],
    layout_type: str,
    layout_columns: list[str],
    column_types: dict[str, str],
    filter_columns: list[str],
    rows_per_group_sample: int,
) -> list[PartitionInfo]:
    """Build partition-aware sorted slices and row-group stats."""

    if (
        partition_strategy == "none"
        or not partition_columns
        or any(column not in sample_frame.columns for column in partition_columns)
    ):
        sorted_frame = sort_sample_by_layout(
            sample_frame,
            layout_type=layout_type,
            layout_columns=layout_columns,
            column_types=column_types,
        )
        row_group_stats = build_row_group_stats(
            sorted_frame,
            filter_columns=filter_columns,
            column_types=column_types,
            rows_per_group_sample=rows_per_group_sample,
        ) or [RowGroupStat(row_count=len(sorted_frame), column_bounds={})]
        return [
            PartitionInfo(
                key=(),
                partition_columns=tuple(),
                frame=sorted_frame,
                row_count=len(sorted_frame),
                row_group_stats=row_group_stats,
            )
        ]

    partitions: list[PartitionInfo] = []
    grouped = sample_frame.groupby(partition_columns, dropna=False, sort=False)
    for key, partition_frame in grouped:
        partition_key = key if isinstance(key, tuple) else (key,)
        sorted_frame = sort_sample_by_layout(
            partition_frame.reset_index(drop=True),
            layout_type=layout_type,
            layout_columns=layout_columns,
            column_types=column_types,
        )
        row_group_stats = build_row_group_stats(
            sorted_frame,
            filter_columns=filter_columns,
            column_types=column_types,
            rows_per_group_sample=max(1, min(rows_per_group_sample, len(sorted_frame))),
        ) or [RowGroupStat(row_count=len(sorted_frame), column_bounds={})]
        partitions.append(
            PartitionInfo(
                key=partition_key,
                partition_columns=tuple(partition_columns),
                frame=sorted_frame,
                row_count=len(sorted_frame),
                row_group_stats=row_group_stats,
            )
        )
    return partitions


def touched_partition_infos(
    *,
    partition_infos: list[PartitionInfo],
    predicates: list[PredicateRecord],
    column_types: dict[str, str],
) -> list[PartitionInfo]:
    """Return partitions touched by query predicates on the partition columns."""

    if not partition_infos:
        return []

    partition_columns = list(partition_infos[0].partition_columns)
    constrained_columns = {
        predicate.column for predicate in predicates if predicate.column in partition_columns
    }
    if not constrained_columns:
        return partition_infos

    touched: list[PartitionInfo] = []
    for partition in partition_infos:
        mask = partition_predicate_mask(
            partition.frame,
            [predicate for predicate in predicates if predicate.column in constrained_columns],
            column_types,
        )
        if mask is not None and bool(mask.any()):
            touched.append(partition)
    return touched


def sort_sample_by_layout(
    frame: pd.DataFrame,
    *,
    layout_type: str,
    layout_columns: list[str],
    column_types: dict[str, str],
) -> pd.DataFrame:
    """Sort a sample once according to the selected layout.

    `no layout` preserves incoming order.
    `linear` performs a lexicographic sort on 64-bit encoded column keys.
    """

    if layout_type == "no layout" or not layout_columns:
        return frame.reset_index(drop=True)
    if layout_type != "linear":
        raise ValueError(f"Unsupported score_v1 layout type: {layout_type}")

    sort_keys: list[np.ndarray] = []
    order_fields: list[str] = []
    for index, column in enumerate(layout_columns):
        if column not in frame.columns:
            continue
        encoded, valid = encode_series_to_u64(frame[column], column_types.get(column, "string"))
        sort_keys.extend([valid.astype(np.uint8), encoded])
        order_fields.extend([f"{column}_{index}_valid", f"{column}_{index}_value"])

    if not sort_keys:
        return frame.reset_index(drop=True)

    structured = np.rec.fromarrays(sort_keys, names=order_fields)
    ordering = np.argsort(structured, order=order_fields, kind="stable")
    return frame.iloc[ordering].reset_index(drop=True)


def estimate_rows_per_group_sample(
    *,
    sample_frame: pd.DataFrame,
    dataset_row_count: int,
    sample_ratio: float,
    default_row_group_bytes: int,
) -> int:
    """Estimate how many sampled rows correspond to one full parquet row group."""

    if sample_frame.empty:
        return 1

    average_row_bytes = max(
        int(math.ceil(sample_frame.memory_usage(index=False, deep=True).sum() / len(sample_frame))),
        1,
    )
    full_rows_per_group = max(1, default_row_group_bytes // average_row_bytes)
    effective_ratio = sample_ratio if sample_ratio > 0 else len(sample_frame) / max(dataset_row_count, 1)
    return max(1, int(round(full_rows_per_group * effective_ratio)))


def build_row_group_stats(
    frame: pd.DataFrame,
    *,
    filter_columns: list[str],
    column_types: dict[str, str],
    rows_per_group_sample: int,
) -> list[RowGroupStat]:
    """Build min/max summaries per simulated parquet row group."""

    if frame.empty:
        return []

    encoded_cache = {
        column: encode_series_to_u64(frame[column], column_types.get(column, "string"))
        for column in filter_columns
        if column in frame.columns
    }
    row_group_stats: list[RowGroupStat] = []
    for start in range(0, len(frame), rows_per_group_sample):
        end = min(start + rows_per_group_sample, len(frame))
        bounds: dict[str, tuple[int, int] | None] = {}
        for column, (encoded, valid) in encoded_cache.items():
            group_values = encoded[start:end]
            group_valid = valid[start:end]
            if not group_valid.any():
                bounds[column] = None
                continue
            valid_values = group_values[group_valid]
            bounds[column] = (int(valid_values.min()), int(valid_values.max()))
        row_group_stats.append(
            RowGroupStat(
                row_count=end - start,
                column_bounds=bounds,
            )
        )
    return row_group_stats


def query_to_column_ranges(
    predicates: list[PredicateRecord],
    column_types: dict[str, str],
) -> dict[str, EncodedRange]:
    """Convert supported SQL predicates into encoded per-column ranges."""

    merged: dict[str, EncodedRange] = {}
    for predicate in predicates:
        inferred_type = column_types.get(predicate.column)
        if not inferred_type:
            continue
        predicate_range = predicate_to_encoded_range(predicate, inferred_type)
        if predicate_range is None:
            continue
        merged[predicate.column] = intersect_ranges(
            merged.get(predicate.column),
            predicate_range,
        )
    return merged


def predicate_to_encoded_range(
    predicate: PredicateRecord,
    inferred_type: str,
) -> EncodedRange | None:
    """Build an encoded range for one predicate."""

    if predicate.predicate_type == "equality" and predicate.value_sql is not None:
        encoded_value = encode_scalar_to_int(
            parse_literal_value(predicate.value_sql, inferred_type),
            inferred_type,
        )
        return EncodedRange(lower=encoded_value, upper=encoded_value)

    if predicate.predicate_type == "in_list" and predicate.values_sql:
        encoded_values = sorted(
            encode_scalar_to_int(parse_literal_value(value_sql, inferred_type), inferred_type)
            for value_sql in predicate.values_sql
        )
        return EncodedRange(
            lower=encoded_values[0],
            upper=encoded_values[-1],
            lower_inclusive=True,
            upper_inclusive=True,
        )

    if predicate.predicate_type == "range":
        lower = None
        upper = None
        if predicate.lower_sql is not None:
            lower = encode_scalar_to_int(
                parse_literal_value(predicate.lower_sql, inferred_type),
                inferred_type,
            )
        if predicate.upper_sql is not None:
            upper = encode_scalar_to_int(
                parse_literal_value(predicate.upper_sql, inferred_type),
                inferred_type,
            )
        return EncodedRange(
            lower=lower,
            upper=upper,
            lower_inclusive=predicate.lower_inclusive if predicate.lower_inclusive is not None else True,
            upper_inclusive=predicate.upper_inclusive if predicate.upper_inclusive is not None else True,
        )

    if predicate.predicate_type == "prefix" and predicate.pattern_value is not None:
        lower_bytes = prefix_bytes(predicate.pattern_value)
        upper_bytes = next_prefix_bytes(lower_bytes)
        return EncodedRange(
            lower=bytes_to_u64_int(lower_bytes),
            upper=bytes_to_u64_int(upper_bytes) if upper_bytes is not None else None,
            lower_inclusive=True,
            upper_inclusive=False,
        )

    return None


def intersect_ranges(current: EncodedRange | None, new: EncodedRange) -> EncodedRange:
    """Intersect two encoded ranges on the same column."""

    if current is None:
        return new

    lower = current.lower
    lower_inclusive = current.lower_inclusive
    if new.lower is not None and (
        lower is None
        or new.lower > lower
        or (new.lower == lower and not new.lower_inclusive and lower_inclusive)
    ):
        lower = new.lower
        lower_inclusive = new.lower_inclusive

    upper = current.upper
    upper_inclusive = current.upper_inclusive
    if new.upper is not None and (
        upper is None
        or new.upper < upper
        or (new.upper == upper and not new.upper_inclusive and upper_inclusive)
    ):
        upper = new.upper
        upper_inclusive = new.upper_inclusive

    return EncodedRange(
        lower=lower,
        upper=upper,
        lower_inclusive=lower_inclusive,
        upper_inclusive=upper_inclusive,
    )


def row_group_matches_query(
    row_group: RowGroupStat,
    query_ranges: dict[str, EncodedRange],
) -> bool:
    """Return True when a row group cannot be pruned by any predicate range."""

    for column, query_range in query_ranges.items():
        bounds = row_group.column_bounds.get(column)
        if bounds is None:
            return False
        group_min, group_max = bounds
        if query_range.lower is not None:
            if query_range.lower_inclusive:
                if group_max < query_range.lower:
                    return False
            elif group_max <= query_range.lower:
                return False
        if query_range.upper is not None:
            if query_range.upper_inclusive:
                if group_min > query_range.upper:
                    return False
            elif group_min >= query_range.upper:
                return False
    return True


def scale_sample_rows_to_full(
    *,
    sample_rows_read: int,
    sample_ratio: float,
    full_row_count: int,
) -> int:
    """Scale sampled rows read back to an estimated full-dataset row count."""

    if sample_rows_read <= 0:
        return 0
    if sample_ratio <= 0:
        return min(sample_rows_read, full_row_count)
    return min(full_row_count, max(1, int(round(sample_rows_read / sample_ratio))))


def encode_series_to_u64(series: pd.Series, inferred_type: str) -> tuple[np.ndarray, np.ndarray]:
    """Encode one series into sortable 64-bit values and a validity mask."""

    if inferred_type == "integer":
        numeric = pd.to_numeric(series, errors="coerce")
        valid = ~pd.isna(numeric)
        values = numeric.fillna(0).astype("int64").to_numpy()
        encoded = values.view("uint64") ^ UINT64_SIGN_MASK
        return encoded.astype(np.uint64), valid.to_numpy()

    if inferred_type == "float":
        numeric = pd.to_numeric(series, errors="coerce").astype("float64")
        valid = ~pd.isna(numeric)
        bits = numeric.fillna(0.0).to_numpy().view("uint64")
        negative_mask = (bits & UINT64_SIGN_MASK) != 0
        encoded = np.where(negative_mask, np.bitwise_xor(bits, UINT64_ALL_ONES), bits ^ UINT64_SIGN_MASK)
        return encoded.astype(np.uint64), valid.to_numpy()

    if inferred_type == "datetime":
        timestamps = pd.to_datetime(series, errors="coerce")
        valid = ~pd.isna(timestamps)
        values = timestamps.fillna(pd.Timestamp(0)).astype("int64").to_numpy()
        encoded = values.view("uint64") ^ UINT64_SIGN_MASK
        return encoded.astype(np.uint64), valid.to_numpy()

    string_values = series.astype("string")
    valid = ~string_values.isna()
    encoded = np.array(
        [
            bytes_to_u64(str(value).encode("utf-8")[:8].ljust(8, b"\x00"))
            if not pd.isna(value)
            else np.uint64(0)
            for value in string_values
        ],
        dtype=np.uint64,
    )
    return encoded, valid.to_numpy()


def encode_scalar_to_int(value: object, inferred_type: str) -> int:
    """Encode one scalar into a sortable 64-bit integer."""

    if inferred_type == "integer":
        array = np.array([int(value)], dtype=np.int64)
        return int((array.view(np.uint64) ^ UINT64_SIGN_MASK)[0])
    if inferred_type == "float":
        array = np.array([float(value)], dtype=np.float64)
        bits = array.view(np.uint64)
        negative = (bits & UINT64_SIGN_MASK) != 0
        encoded = np.where(negative, np.bitwise_xor(bits, UINT64_ALL_ONES), bits ^ UINT64_SIGN_MASK)
        return int(encoded[0])
    if inferred_type == "datetime":
        timestamp = pd.Timestamp(value)
        array = np.array([timestamp.value], dtype=np.int64)
        return int((array.view(np.uint64) ^ UINT64_SIGN_MASK)[0])
    return bytes_to_u64_int(prefix_bytes(str(value)))


def parse_literal_value(raw_sql: str, inferred_type: str) -> object:
    """Parse a raw SQL literal into a Python scalar aligned with the column type."""

    literal = raw_sql.strip()
    if literal.upper().startswith("DATE "):
        literal = literal[5:].strip()
    else:
        cast_literal = _extract_cast_literal(literal)
        if cast_literal is not None:
            literal = cast_literal

    if inferred_type == "integer":
        return int(_strip_sql_quotes(literal))
    if inferred_type == "float":
        return float(_strip_sql_quotes(literal))
    if inferred_type == "datetime":
        return pd.Timestamp(_strip_sql_quotes(literal))
    return _strip_sql_quotes(literal)


def _strip_sql_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def _extract_cast_literal(value: str) -> str | None:
    """Extract the inner literal from `CAST('x' AS DATE)`-style SQL."""

    match = re.fullmatch(
        r"CAST\(\s*('(?:''|[^'])*')\s+AS\s+(?:DATE|TIMESTAMP)\s*\)",
        value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1)


def prefix_bytes(prefix: str) -> bytes:
    return prefix.encode("utf-8")[:8].ljust(8, b"\x00")


def next_prefix_bytes(value: bytes) -> bytes | None:
    """Return the next lexicographic byte prefix for an exclusive upper bound."""

    mutable = bytearray(value)
    for index in range(len(mutable) - 1, -1, -1):
        if mutable[index] < 0xFF:
            mutable[index] += 1
            for tail_index in range(index + 1, len(mutable)):
                mutable[tail_index] = 0
            return bytes(mutable)
    return None


def partition_predicate_mask(
    frame: pd.DataFrame,
    predicates: list[PredicateRecord],
    column_types: dict[str, str],
) -> pd.Series | None:
    """Apply predicates on a frame to identify touched partitions."""

    if frame.empty:
        return None

    mask = pd.Series(True, index=frame.index, dtype=bool)
    applied = False
    for predicate in predicates:
        predicate_mask = predicate_mask_for_frame(frame, predicate, column_types)
        if predicate_mask is None:
            continue
        mask &= predicate_mask
        applied = True
    return mask if applied else None


def predicate_mask_for_frame(
    frame: pd.DataFrame,
    predicate: PredicateRecord,
    column_types: dict[str, str],
) -> pd.Series | None:
    """Build a boolean mask for one predicate against a dataframe."""

    if predicate.column not in frame.columns:
        return None

    inferred_type = column_types.get(predicate.column, "string")
    series = coerce_series_for_predicate(frame[predicate.column], inferred_type)

    try:
        if predicate.predicate_type == "equality" and predicate.value_sql is not None:
            value = parse_literal_value(predicate.value_sql, inferred_type)
            return series.eq(value).fillna(False)

        if predicate.predicate_type == "not_equal" and predicate.value_sql is not None:
            value = parse_literal_value(predicate.value_sql, inferred_type)
            return series.ne(value).fillna(False)

        if predicate.predicate_type == "in_list" and predicate.values_sql:
            values = [parse_literal_value(value_sql, inferred_type) for value_sql in predicate.values_sql]
            return series.isin(values).fillna(False)

        if predicate.predicate_type in {"prefix", "suffix", "contains"} and predicate.pattern_value is not None:
            string_series = frame[predicate.column].astype("string")
            if predicate.predicate_type == "prefix":
                return string_series.fillna("").str.startswith(predicate.pattern_value)
            if predicate.predicate_type == "suffix":
                return string_series.fillna("").str.endswith(predicate.pattern_value)
            return string_series.fillna("").str.contains(predicate.pattern_value, regex=False)

        if predicate.predicate_type == "range":
            mask = pd.Series(True, index=frame.index, dtype=bool)
            if predicate.lower_sql is not None:
                lower = parse_literal_value(predicate.lower_sql, inferred_type)
                lower_mask = series.ge(lower) if predicate.lower_inclusive else series.gt(lower)
                mask &= lower_mask.fillna(False)
            if predicate.upper_sql is not None:
                upper = parse_literal_value(predicate.upper_sql, inferred_type)
                upper_mask = series.le(upper) if predicate.upper_inclusive else series.lt(upper)
                mask &= upper_mask.fillna(False)
            return mask
    except Exception:
        return None

    return None


def coerce_series_for_predicate(series: pd.Series, inferred_type: str) -> pd.Series:
    """Coerce a pandas series to a predicate-compatible dtype."""

    if inferred_type == "integer":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if inferred_type == "float":
        return pd.to_numeric(series, errors="coerce")
    if inferred_type == "datetime":
        return pd.to_datetime(series, errors="coerce")
    return series.astype("string")


def bytes_to_u64(value: bytes) -> np.uint64:
    return np.uint64(int.from_bytes(value[:8].ljust(8, b"\x00"), "big", signed=False))


def bytes_to_u64_int(value: bytes) -> int:
    return int(bytes_to_u64(value))
