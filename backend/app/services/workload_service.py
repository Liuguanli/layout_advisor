"""Service for SQL workload ingestion and analysis."""

from __future__ import annotations

from collections import Counter
from itertools import combinations
from pathlib import Path

import pandas as pd

from app.config.workload_catalog import WORKLOAD_CATALOG
from app.models.workload import (
    PairFrequency,
    PredicateCombinationFrequency,
    StaticWorkloadItem,
    WorkloadCatalogResponse,
    WorkloadSummary,
    WorkloadUploadResponse,
)
from app.services.layout_score_v1 import parse_literal_value
from app.utils.sql_parser import PredicateRecord, parse_query_predicates


class WorkloadService:
    """In-memory manager for static SQL workload artifacts."""

    def __init__(self) -> None:
        self._query_predicates: list[list[PredicateRecord]] = []
        self._catalog: list[StaticWorkloadItem] = [
            StaticWorkloadItem(**item) for item in WORKLOAD_CATALOG
        ]

    def ingest_workload(self, content: str) -> WorkloadUploadResponse:
        """Parse workload file content where each non-empty line is a query."""

        parsed_queries: list[list[PredicateRecord]] = []
        failed_queries = 0

        for line in content.splitlines():
            query = line.strip()
            if not query:
                continue

            try:
                predicates = parse_query_predicates(query)
            except ValueError:
                failed_queries += 1
                continue

            parsed_queries.append(predicates)

            self._query_predicates = parsed_queries
        return WorkloadUploadResponse(
            imported_queries=len(parsed_queries),
            failed_queries=failed_queries,
        )

    def get_query_predicates(self) -> list[list[PredicateRecord]]:
        """Return the parsed workload predicates for downstream evaluators."""

        if not self._query_predicates:
            raise ValueError("No workload has been selected yet.")
        return [list(predicates) for predicates in self._query_predicates]

    def list_workloads(self) -> WorkloadCatalogResponse:
        """Return configured static workloads."""

        return WorkloadCatalogResponse(workloads=self._catalog)

    def select_workload(self, workload_id: str) -> WorkloadUploadResponse:
        """Load and parse workload file from static catalog by id."""

        selected = next(
            (item for item in self._catalog if item.workload_id == workload_id),
            None,
        )
        if selected is None:
            raise ValueError(f"Unknown workload_id: {workload_id}")

        workload_path = Path(selected.file_path)
        if not workload_path.exists():
            raise ValueError(f"Configured workload file does not exist: {workload_path}")
        if workload_path.is_dir():
            raise ValueError(
                f"Configured workload path must be a text file, got directory: {workload_path}"
            )

        content = workload_path.read_text(encoding="utf-8")
        if not content.strip():
            raise ValueError(f"Configured workload file is empty: {workload_path}")

        return self.ingest_workload(content)

    def summarize(
        self,
        sample_frame: pd.DataFrame | None = None,
        column_types: dict[str, str] | None = None,
    ) -> WorkloadSummary:
        """Compute workload-level statistics from parsed query predicates."""

        if not self._query_predicates:
            raise ValueError("No workload has been selected yet.")

        predicate_type_counter: Counter[str] = Counter()
        column_counter: Counter[str] = Counter()
        per_column_predicate_type_counter: dict[str, Counter[str]] = {}
        combination_counter: Counter[tuple[str, ...]] = Counter()
        pair_counter: Counter[tuple[str, str]] = Counter()
        complexity_counter: Counter[int] = Counter()
        predicate_selectivity_sum: Counter[str] = Counter()
        predicate_selectivity_count: Counter[str] = Counter()
        query_selectivity_sum: Counter[str] = Counter()
        query_selectivity_count: Counter[str] = Counter()

        for predicates in self._query_predicates:
            complexity_counter[len(predicates)] += 1

            query_columns: list[str] = []
            for record in predicates:
                predicate_type_counter[record.predicate_type] += 1
                column_counter[record.column] += 1
                if record.column not in per_column_predicate_type_counter:
                    per_column_predicate_type_counter[record.column] = Counter()
                per_column_predicate_type_counter[record.column][record.predicate_type] += 1
                query_columns.append(record.column)

            unique_columns = tuple(sorted(set(query_columns)))
            if unique_columns:
                combination_counter[unique_columns] += 1

            for col_a, col_b in combinations(unique_columns, 2):
                pair_counter[(col_a, col_b)] += 1

            if sample_frame is not None and column_types is not None and not sample_frame.empty:
                query_selectivity = _estimate_query_selectivity(
                    sample_frame,
                    predicates,
                    column_types,
                )
                for predicate in predicates:
                    predicate_selectivity = _estimate_predicate_selectivity(
                        sample_frame,
                        predicate,
                        column_types,
                    )
                    if predicate_selectivity is not None:
                        predicate_selectivity_sum[predicate.column] += predicate_selectivity
                        predicate_selectivity_count[predicate.column] += 1
                if query_selectivity is not None:
                    for column in set(query_columns):
                        query_selectivity_sum[column] += query_selectivity
                        query_selectivity_count[column] += 1

        top_combinations = [
            PredicateCombinationFrequency(columns=list(combo), count=count)
            for combo, count in combination_counter.most_common(12)
        ]
        top_pairs = [
            PairFrequency(column_a=pair[0], column_b=pair[1], count=count)
            for pair, count in pair_counter.most_common(10)
        ]

        return WorkloadSummary(
            total_queries=len(self._query_predicates),
            predicate_type_distribution=dict(predicate_type_counter),
            per_column_filter_frequency=dict(column_counter),
            per_column_predicate_type_distribution={
                column: dict(counter)
                for column, counter in per_column_predicate_type_counter.items()
            },
            per_column_avg_predicate_selectivity={
                column: round(predicate_selectivity_sum[column] / predicate_selectivity_count[column], 4)
                for column in predicate_selectivity_count
                if predicate_selectivity_count[column] > 0
            },
            per_column_avg_query_selectivity={
                column: round(query_selectivity_sum[column] / query_selectivity_count[column], 4)
                for column in query_selectivity_count
                if query_selectivity_count[column] > 0
            },
            top_predicate_combinations=top_combinations,
            top_cooccurring_filter_pairs=top_pairs,
            query_complexity_distribution={
                str(num_predicates): count
                for num_predicates, count in sorted(complexity_counter.items())
            },
        )


def _estimate_query_selectivity(
    frame: pd.DataFrame,
    predicates: list[PredicateRecord],
    column_types: dict[str, str],
) -> float | None:
    """Estimate full-query selectivity on a sampled frame.

    The returned ratio is `matched_rows / sample_rows`, so lower is more selective.
    """

    if frame.empty:
        return None

    mask = pd.Series(True, index=frame.index, dtype=bool)
    applied = False
    for predicate in predicates:
        predicate_mask = _predicate_mask(frame, predicate, column_types)
        if predicate_mask is None:
            continue
        mask &= predicate_mask
        applied = True

    if not applied:
        return None
    return float(mask.mean())


def _estimate_predicate_selectivity(
    frame: pd.DataFrame,
    predicate: PredicateRecord,
    column_types: dict[str, str],
) -> float | None:
    """Estimate single-predicate selectivity on a sampled frame."""

    if frame.empty:
        return None
    predicate_mask = _predicate_mask(frame, predicate, column_types)
    if predicate_mask is None:
        return None
    return float(predicate_mask.mean())


def _predicate_mask(
    frame: pd.DataFrame,
    predicate: PredicateRecord,
    column_types: dict[str, str],
) -> pd.Series | None:
    """Build a boolean mask for one supported predicate."""

    if predicate.column not in frame.columns:
        return None

    inferred_type = column_types.get(predicate.column, "string")
    series = frame[predicate.column]

    if predicate.predicate_type in {"prefix", "suffix", "contains"} and predicate.pattern_value is not None:
        string_series = series.astype("string")
        if predicate.predicate_type == "prefix":
            return string_series.fillna("").str.startswith(predicate.pattern_value)
        if predicate.predicate_type == "suffix":
            return string_series.fillna("").str.endswith(predicate.pattern_value)
        return string_series.fillna("").str.contains(predicate.pattern_value, regex=False)

    try:
        typed_series = _coerce_series(series, inferred_type)
        if predicate.predicate_type == "equality" and predicate.value_sql is not None:
            value = parse_literal_value(predicate.value_sql, inferred_type)
            return typed_series.eq(value).fillna(False)

        if predicate.predicate_type == "not_equal" and predicate.value_sql is not None:
            value = parse_literal_value(predicate.value_sql, inferred_type)
            return typed_series.ne(value).fillna(False)

        if predicate.predicate_type == "in_list" and predicate.values_sql:
            values = [parse_literal_value(value_sql, inferred_type) for value_sql in predicate.values_sql]
            return typed_series.isin(values).fillna(False)

        if predicate.predicate_type == "range":
            mask = pd.Series(True, index=series.index, dtype=bool)
            if predicate.lower_sql is not None:
                lower = parse_literal_value(predicate.lower_sql, inferred_type)
                lower_mask = typed_series.ge(lower) if predicate.lower_inclusive else typed_series.gt(lower)
                mask &= lower_mask.fillna(False)
            if predicate.upper_sql is not None:
                upper = parse_literal_value(predicate.upper_sql, inferred_type)
                upper_mask = typed_series.le(upper) if predicate.upper_inclusive else typed_series.lt(upper)
                mask &= upper_mask.fillna(False)
            return mask
    except Exception:
        return None

    return None


def _coerce_series(series: pd.Series, inferred_type: str) -> pd.Series:
    """Coerce a series to a type compatible with predicate evaluation."""

    if inferred_type == "integer":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if inferred_type == "float":
        return pd.to_numeric(series, errors="coerce")
    if inferred_type == "datetime":
        return pd.to_datetime(series, errors="coerce")
    return series.astype("string")
