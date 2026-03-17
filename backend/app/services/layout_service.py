"""Layout estimation and workload-aware evaluation services.

The existing `/estimate` flow is preserved as a lightweight placeholder API for
the current frontend. The new `/evaluate` flow adds explicit query-level and
layout-level metrics so later estimator implementations can plug in without
changing the API contract.
"""

from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass, replace

import pandas as pd

from app.models.layout import (
    LayoutEstimateItem,
    LayoutEstimateRequest,
    LayoutEstimateResponse,
    LayoutEvaluation,
    LayoutEvaluationRequest,
    LayoutEvaluationResponse,
    LayoutPermutationCandidate,
    MockExecutionCandidate,
    MockExecutionRequest,
    MockExecutionResponse,
    MockExecutionResult,
    QueryEstimate,
    ScoreWeights,
)
from app.services.layout_score_v1 import (
    SCORE_V1_ALGORITHM_NAME,
    SamplePruningScoreV1Estimator,
    estimate_rows_per_group_sample,
)
from app.services.dataset_service import DatasetService
from app.services.workload_service import WorkloadService
from app.utils.sql_parser import PredicateRecord

PLACEHOLDER_ALGORITHM_NAME = "placeholder_v1"
MOCK_QUERY_ESTIMATOR_NAME = "mock_query_metrics_v1"
MOCK_ACTUAL_RUNNER_NAME = "mock_actual_benchmark_v1"
DEFAULT_ROW_GROUP_BYTES = 128 * 1024 * 1024
LAYOUT_OPTIONS = {"no layout", "zorder", "linear", "hilbert"}
PARTITION_STRATEGIES = {"none", "value"}


@dataclass
class EstimationContext:
    """Context exposed to legacy placeholder estimators."""

    dataset_id: str | None
    dataset_row_count: int
    workload_loaded: bool
    workload_query_count: int


@dataclass
class QueryEvaluationContext:
    """Context shared by workload-aware query estimators."""

    dataset_id: str | None
    dataset_row_count: int
    dataset_column_count: int
    dataset_profile_sample_size: int
    sample_frame: pd.DataFrame
    column_types: dict[str, str]
    total_bytes: int
    total_row_groups: int
    sample_ratio: float
    default_row_group_bytes: int
    query_predicates: list[list[PredicateRecord]]
    partition_strategy: str
    partition_columns: list[str]


class BaseLayoutEstimator:
    """Abstract estimator interface for future single-score models."""

    algorithm_name = "base"

    def estimate(
        self,
        layout_type: str,
        candidate: LayoutPermutationCandidate,
        context: EstimationContext,
    ) -> LayoutEstimateItem:
        """Estimate one candidate for the legacy placeholder endpoint."""

        raise NotImplementedError


class BaseQueryAccessEstimator:
    """Abstract interface for workload-aware per-query access estimates."""

    algorithm_name = "base_query_estimator"

    def estimate_queries(
        self,
        layout_type: str,
        candidate: LayoutPermutationCandidate,
        context: QueryEvaluationContext,
    ) -> list[QueryEstimate]:
        """Return one `QueryEstimate` per parsed query."""

        raise NotImplementedError


class PlaceholderRandomEstimator(BaseLayoutEstimator):
    """Stable pseudo-random estimator used before real cost models exist."""

    algorithm_name = PLACEHOLDER_ALGORITHM_NAME

    def estimate(
        self,
        layout_type: str,
        candidate: LayoutPermutationCandidate,
        context: EstimationContext,
    ) -> LayoutEstimateItem:
        signature = "|".join(
            [
                self.algorithm_name,
                context.dataset_id or "no_dataset",
                str(context.dataset_row_count),
                str(context.workload_query_count),
                layout_type,
                candidate.key,
                ",".join(candidate.columns),
            ]
        )
        seed = int(hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed)

        base_cost = rng.uniform(15.0, 95.0)
        column_penalty = len(candidate.columns) * 2.75
        workload_penalty = min(context.workload_query_count / 100, 12.0)
        row_scale = min(context.dataset_row_count / 1_000_000, 8.0)

        if layout_type == "no layout":
            estimated_cost = base_cost + workload_penalty + row_scale
        elif layout_type == "zorder":
            estimated_cost = base_cost * 0.82 + column_penalty + workload_penalty * 0.7
        elif layout_type == "hilbert":
            estimated_cost = base_cost * 0.86 + column_penalty * 1.1 + workload_penalty * 0.75
        elif layout_type == "linear":
            estimated_cost = base_cost * 0.92 + column_penalty * 0.95 + workload_penalty * 0.9
        else:
            estimated_cost = base_cost + column_penalty + workload_penalty + row_scale

        estimated_cost = round(max(estimated_cost, 0.1), 3)
        return LayoutEstimateItem(
            estimate_id=f"{layout_type}::{candidate.key}",
            layout_type=layout_type,
            candidate_key=candidate.key,
            column_order=candidate.columns,
            estimated_cost=estimated_cost,
            algorithm=self.algorithm_name,
            notes="Placeholder estimator with stable pseudo-random scores.",
        )


class DeterministicMockQueryEstimator(BaseQueryAccessEstimator):
    """Workload-aware mock estimator with deterministic, comparable metrics.

    This is intentionally not a physical parquet estimator. It produces
    deterministic query-level estimates from:
    - dataset size and sample ratio
    - predicate complexity and types
    - overlap between predicate columns and layout columns

    The contract is the important part: later a real estimator can replace this
    class while keeping the same output structure.
    """

    algorithm_name = MOCK_QUERY_ESTIMATOR_NAME

    def estimate_queries(
        self,
        layout_type: str,
        candidate: LayoutPermutationCandidate,
        context: QueryEvaluationContext,
    ) -> list[QueryEstimate]:
        avg_record_bytes = max(
            context.total_bytes // max(context.dataset_row_count, 1),
            1,
        )
        rows_per_group = max(
            1,
            math.ceil(context.dataset_row_count / max(context.total_row_groups, 1)),
        )
        candidate_columns = set(candidate.columns)

        estimates: list[QueryEstimate] = []
        for index, predicates in enumerate(context.query_predicates, start=1):
            predicate_columns = sorted({predicate.column for predicate in predicates})
            baseline_records = self._estimate_baseline_records(
                layout_type=layout_type,
                candidate=candidate,
                predicate_columns=predicate_columns,
                predicates=predicates,
                total_records=context.dataset_row_count,
                sample_ratio=context.sample_ratio,
                query_index=index,
            )
            estimated_records_read = baseline_records
            partition_columns = set(context.partition_columns)
            partition_overlap_ratio = (
                len(partition_columns & set(predicate_columns))
                / max(len(partition_columns), 1)
                if context.partition_strategy != "none" and partition_columns
                else 0.0
            )
            if context.partition_strategy != "none" and partition_columns:
                partition_reduction = min(
                    (0.18 + 0.22 * partition_overlap_ratio) * max(partition_overlap_ratio, 0.1),
                    0.7,
                )
                estimated_records_read = max(
                    1,
                    int(round(estimated_records_read * (1.0 - partition_reduction))),
                )

            if layout_type != "no layout":
                overlap_ratio = (
                    len(candidate_columns & set(predicate_columns))
                    / max(len(predicate_columns), 1)
                )
                lead_bonus = (
                    0.12
                    if candidate.columns and candidate.columns[0] in predicate_columns
                    else 0.0
                )
                layout_gain = {
                    "linear": 0.24,
                    "zorder": 0.31,
                    "hilbert": 0.34,
                }.get(layout_type, 0.0)
                sample_confidence = 0.7 + 0.3 * min(context.sample_ratio / 0.05, 1.0)
                reduction = min(
                    (layout_gain + lead_bonus) * max(overlap_ratio, 0.08) * sample_confidence,
                    0.72,
                )
                estimated_records_read = max(
                    1,
                    int(round(baseline_records * (1.0 - reduction))),
                )

            estimated_bytes_read = max(estimated_records_read * avg_record_bytes, 1)
            estimated_row_groups_read = min(
                context.total_row_groups,
                max(1, math.ceil(estimated_records_read / rows_per_group)),
            )
            estimates.append(
                QueryEstimate(
                    query_id=f"q{index:04d}",
                    predicate_columns=predicate_columns,
                    estimated_records_read=estimated_records_read,
                    estimated_bytes_read=estimated_bytes_read,
                    estimated_row_groups_read=estimated_row_groups_read,
                    benefit_vs_baseline=0.0,
                )
            )

        return estimates

    def _estimate_baseline_records(
        self,
        *,
        layout_type: str,
        candidate: LayoutPermutationCandidate,
        predicate_columns: list[str],
        predicates: list[PredicateRecord],
        total_records: int,
        sample_ratio: float,
        query_index: int,
    ) -> int:
        selectivity = 1.0
        for predicate in predicates:
            if predicate.predicate_type == "equality":
                selectivity *= 0.09
            elif predicate.predicate_type == "in_list":
                selectivity *= 0.14
            elif predicate.predicate_type == "prefix":
                selectivity *= 0.18
            elif predicate.predicate_type == "suffix":
                selectivity *= 0.24
            elif predicate.predicate_type == "contains":
                selectivity *= 0.26
            elif predicate.predicate_type == "range":
                selectivity *= 0.28
            elif predicate.predicate_type == "not_equal":
                selectivity *= 0.52
            else:
                selectivity *= 0.4

        complexity_factor = max(0.62, 1.0 - max(len(predicates) - 1, 0) * 0.05)
        layout_factor = 1.0 + min(len(candidate.columns), 4) * 0.015
        sample_factor = 1.0 + max(0.0, 0.05 - sample_ratio) * 1.5
        noise = _deterministic_ratio(
            [
                self.algorithm_name,
                layout_type,
                candidate.key,
                ",".join(predicate_columns),
                str(query_index),
            ],
            low=0.92,
            high=1.08,
        )
        baseline_ratio = min(
            max(selectivity * complexity_factor * layout_factor * sample_factor * noise, 0.01),
            0.95,
        )
        return max(1, int(round(total_records * baseline_ratio)))


class LayoutService:
    """Facade around placeholder and workload-aware layout evaluators."""

    def __init__(
        self,
        dataset_service: DatasetService,
        workload_service: WorkloadService,
    ) -> None:
        self._dataset_service = dataset_service
        self._workload_service = workload_service
        self._estimator: BaseLayoutEstimator = PlaceholderRandomEstimator()
        self._query_estimator: BaseQueryAccessEstimator = DeterministicMockQueryEstimator()
        self._score_v1_estimator = SamplePruningScoreV1Estimator()

    def estimate(self, payload: LayoutEstimateRequest) -> LayoutEstimateResponse:
        """Estimate selected layout candidates using the legacy placeholder API."""

        self._validate_layout_selection(
            payload.layout_types,
            payload.selected_candidates,
            payload.partition_strategy,
            payload.partition_columns,
        )

        dataset_summary = self._dataset_service.get_summary()
        if payload.dataset_id and dataset_summary.dataset_id != payload.dataset_id:
            dataset_summary = self._dataset_service.select_dataset(payload.dataset_id)

        workload_loaded = True
        workload_query_count = 0
        try:
            workload_summary = self._workload_service.summarize()
            workload_query_count = workload_summary.total_queries
        except ValueError:
            workload_loaded = False

        context = EstimationContext(
            dataset_id=dataset_summary.dataset_id,
            dataset_row_count=dataset_summary.row_count,
            workload_loaded=workload_loaded,
            workload_query_count=workload_query_count,
        )

        estimates: list[LayoutEstimateItem] = []
        physical_layouts = [layout for layout in payload.layout_types if layout != "no layout"]
        for layout_type in physical_layouts:
            for candidate in payload.selected_candidates:
                estimates.append(self._estimator.estimate(layout_type, candidate, context))

        if "no layout" in payload.layout_types:
            estimates.append(
                self._estimator.estimate(
                    "no layout",
                    LayoutPermutationCandidate(key="no_layout", columns=[]),
                    context,
                )
            )

        estimates.sort(key=lambda item: item.estimated_cost)
        return LayoutEstimateResponse(
            dataset_id=dataset_summary.dataset_id,
            workload_loaded=workload_loaded,
            total_estimates=len(estimates),
            estimates=estimates,
        )

    def evaluate(self, payload: LayoutEvaluationRequest) -> LayoutEvaluationResponse:
        """Evaluate selected layouts using explicit query-level and workload metrics."""

        self._validate_layout_selection(
            payload.layout_types,
            payload.selected_candidates,
            payload.partition_strategy,
            payload.partition_columns,
        )

        dataset_summary = self._dataset_service.get_summary()
        if payload.dataset_id and dataset_summary.dataset_id != payload.dataset_id:
            dataset_summary = self._dataset_service.select_dataset(payload.dataset_id)

        query_predicates = self._workload_service.get_query_predicates()
        row_count = dataset_summary.row_count
        column_count = len(dataset_summary.columns)
        sample_frame = self._dataset_service.get_sample_frame(dataset_summary.profile_sample_size)
        sample_ratio = (
            min(dataset_summary.profile_sample_size / row_count, 1.0)
            if row_count > 0
            else 1.0
        )
        average_record_bytes = max(
            int(math.ceil(sample_frame.memory_usage(index=False, deep=True).sum() / max(len(sample_frame), 1))),
            max(64, column_count * 12 + 24),
        )
        total_bytes = max(row_count * average_record_bytes, 1)
        rows_per_group_sample = estimate_rows_per_group_sample(
            sample_frame=sample_frame,
            dataset_row_count=row_count,
            sample_ratio=sample_ratio,
            default_row_group_bytes=DEFAULT_ROW_GROUP_BYTES,
        )
        total_row_groups = max(1, math.ceil(len(sample_frame) / max(rows_per_group_sample, 1)))

        context = QueryEvaluationContext(
            dataset_id=dataset_summary.dataset_id,
            dataset_row_count=row_count,
            dataset_column_count=column_count,
            dataset_profile_sample_size=dataset_summary.profile_sample_size,
            sample_frame=sample_frame,
            column_types={
                column.name: column.inferred_type for column in dataset_summary.columns
            },
            total_bytes=total_bytes,
            total_row_groups=total_row_groups,
            sample_ratio=sample_ratio,
            default_row_group_bytes=DEFAULT_ROW_GROUP_BYTES,
            query_predicates=query_predicates,
            partition_strategy=(
                payload.partition_strategy
                if payload.partition_strategy != "none" and payload.partition_columns
                else "none"
            ),
            partition_columns=[
                column for column in payload.partition_columns if column in sample_frame.columns
            ],
        )
        score_weights = payload.score_weights or ScoreWeights()
        baseline_context = replace(
            context,
            partition_strategy="none",
            partition_columns=[],
        )
        baseline_candidate = LayoutPermutationCandidate(key="no_layout", columns=[])
        baseline_query_estimates = self._score_v1_estimator.estimate_queries(
            "no layout",
            baseline_candidate,
            baseline_context,
        )

        evaluations: list[LayoutEvaluation] = []
        max_layout_columns = max(
            [len(candidate.columns) for candidate in payload.selected_candidates] + [0]
        )
        for layout_type in payload.layout_types:
            if layout_type == "no layout":
                if context.partition_strategy == "none" or not context.partition_columns:
                    query_estimates = apply_benefit_against_baseline(
                        _copy_query_estimates(baseline_query_estimates),
                        baseline_query_estimates,
                    )
                    algorithm = SCORE_V1_ALGORITHM_NAME
                    notes = "Baseline layout used for benefit comparison."
                else:
                    query_estimates = self._score_v1_estimator.estimate_queries(
                        "no layout",
                        baseline_candidate,
                        context,
                    )
                    query_estimates = apply_benefit_against_baseline(
                        query_estimates,
                        baseline_query_estimates,
                    )
                    algorithm = SCORE_V1_ALGORITHM_NAME
                    notes = "Partition-only design: partition pruning without in-partition layout sorting."
                evaluations.append(
                    aggregate_layout_metrics(
                        evaluation_id=_build_evaluation_id(
                            partition_strategy=context.partition_strategy,
                            partition_columns=context.partition_columns,
                            layout_type="no layout",
                            candidate_key="no_layout",
                        ),
                        candidate_key="no_layout",
                        partition_strategy=context.partition_strategy,
                        partition_columns=context.partition_columns,
                        layout_type="no layout",
                        layout_columns=[],
                        query_estimates=query_estimates,
                        total_records=row_count,
                        total_bytes=total_bytes,
                        total_row_groups=total_row_groups,
                        score_weights=score_weights,
                        max_layout_columns=max_layout_columns,
                        algorithm=algorithm,
                        notes=notes,
                        include_query_estimates=payload.include_query_estimates,
                    )
                )
                continue

            for candidate in payload.selected_candidates:
                estimator = (
                    self._score_v1_estimator
                    if layout_type in self._score_v1_estimator.supported_layouts
                    else self._query_estimator
                )
                candidate_query_estimates = estimator.estimate_queries(
                    layout_type,
                    candidate,
                    context,
                )
                candidate_query_estimates = apply_benefit_against_baseline(
                    candidate_query_estimates,
                    baseline_query_estimates,
                )
                evaluations.append(
                    aggregate_layout_metrics(
                        evaluation_id=_build_evaluation_id(
                            partition_strategy=context.partition_strategy,
                            partition_columns=context.partition_columns,
                            layout_type=layout_type,
                            candidate_key=candidate.key,
                        ),
                        candidate_key=candidate.key,
                        partition_strategy=context.partition_strategy,
                        partition_columns=context.partition_columns,
                        layout_type=layout_type,
                        layout_columns=candidate.columns,
                        query_estimates=candidate_query_estimates,
                        total_records=row_count,
                        total_bytes=total_bytes,
                        total_row_groups=total_row_groups,
                        score_weights=score_weights,
                        max_layout_columns=max_layout_columns,
                        algorithm=estimator.algorithm_name,
                        notes=(
                            "Sample-based pruning simulation."
                            if estimator.algorithm_name == SCORE_V1_ALGORITHM_NAME
                            else "Mock workload-aware metrics. Ready for replacement by a real estimator."
                        ),
                        include_query_estimates=payload.include_query_estimates,
                    )
                )

        evaluations.sort(key=lambda item: _score_value(item), reverse=True)
        return LayoutEvaluationResponse(
            dataset_id=dataset_summary.dataset_id,
            workload_loaded=True,
            total_queries=len(query_predicates),
            total_records=row_count,
            total_bytes=total_bytes,
            total_row_groups=total_row_groups,
            sample_ratio=sample_ratio,
            score_weights=score_weights,
            evaluations=evaluations,
        )

    def mock_execute(self, payload: MockExecutionRequest) -> MockExecutionResponse:
        """Generate deterministic mock actual benchmark results for selected candidates."""

        if not payload.candidates:
            raise ValueError("Select at least one comparison candidate to benchmark.")

        dataset_summary = self._dataset_service.get_summary()
        if payload.dataset_id and dataset_summary.dataset_id != payload.dataset_id:
            dataset_summary = self._dataset_service.select_dataset(payload.dataset_id)

        results: list[MockExecutionResult] = []
        for candidate in payload.candidates:
            signature = "|".join(
                [
                    MOCK_ACTUAL_RUNNER_NAME,
                    dataset_summary.dataset_id or "no_dataset",
                    candidate.evaluation_id,
                    candidate.partition_strategy,
                    ",".join(candidate.partition_columns),
                    candidate.layout_type,
                    ",".join(candidate.layout_columns),
                ]
            )
            jitter = _deterministic_ratio([signature, "jitter"], low=0.92, high=1.12)
            runtime_jitter = _deterministic_ratio([signature, "runtime"], low=0.9, high=1.15)
            record_ratio = min(
                max(candidate.avg_record_read_ratio * jitter, 0.0001),
                1.0,
            )
            row_group_ratio = min(
                max(candidate.avg_row_group_read_ratio * (0.94 + (jitter - 1.0) * 0.7), 0.0001),
                1.0,
            )
            actual_score = max(
                0.001,
                candidate.estimated_score * (0.94 + (jitter - 1.0) * 0.9),
            )
            runtime_ms = max(
                5.0,
                (
                    35.0
                    + record_ratio * 820.0
                    + row_group_ratio * 410.0
                    + candidate.layout_complexity * 22.0
                ) * runtime_jitter,
            )
            results.append(
                MockExecutionResult(
                    evaluation_id=candidate.evaluation_id,
                    partition_strategy=candidate.partition_strategy,
                    partition_columns=list(candidate.partition_columns),
                    layout_type=candidate.layout_type,
                    layout_columns=list(candidate.layout_columns),
                    actual_runtime_ms=round(runtime_ms, 3),
                    actual_records_read_ratio=round(record_ratio, 4),
                    actual_row_group_read_ratio=round(row_group_ratio, 4),
                    actual_score=round(actual_score, 4),
                    runner=MOCK_ACTUAL_RUNNER_NAME,
                    notes="Deterministic mock actual benchmark. Replace with Hudi execution later.",
                )
            )

        results.sort(key=lambda item: item.actual_runtime_ms)
        return MockExecutionResponse(
            dataset_id=dataset_summary.dataset_id,
            total_results=len(results),
            results=results,
        )

    def _validate_layout_selection(
        self,
        layout_types: list[str],
        selected_candidates: list[LayoutPermutationCandidate],
        partition_strategy: str,
        partition_columns: list[str],
    ) -> None:
        if not layout_types:
            raise ValueError("At least one layout type must be selected.")

        invalid_layouts = [layout for layout in layout_types if layout not in LAYOUT_OPTIONS]
        if invalid_layouts:
            raise ValueError(f"Unsupported layout types: {', '.join(invalid_layouts)}")

        if partition_strategy not in PARTITION_STRATEGIES:
            raise ValueError(f"Unsupported partition strategy: {partition_strategy}")

        if partition_strategy == "none" and partition_columns:
            raise ValueError("Partition columns require a non-none partition strategy.")

        physical_layouts = [layout for layout in layout_types if layout != "no layout"]
        if physical_layouts and not selected_candidates:
            raise ValueError("Select at least one permutation candidate for physical layouts.")


def apply_benefit_against_baseline(
    candidate_estimates: list[QueryEstimate],
    baseline_estimates: list[QueryEstimate],
) -> list[QueryEstimate]:
    """Fill `benefit_vs_baseline` for each query estimate.

    Benefit is defined as:
    `1 - candidate_records_read / baseline_records_read`
    """

    baseline_by_query = {estimate.query_id: estimate for estimate in baseline_estimates}
    updated: list[QueryEstimate] = []
    for estimate in candidate_estimates:
        baseline = baseline_by_query.get(estimate.query_id)
        baseline_records = baseline.estimated_records_read if baseline else 0
        benefit = compute_benefit_against_baseline(
            candidate_records_read=estimate.estimated_records_read,
            baseline_records_read=baseline_records,
        )
        updated.append(
            estimate.model_copy(update={"benefit_vs_baseline": round(benefit, 4)})
        )
    return updated


def compute_benefit_against_baseline(
    *,
    candidate_records_read: int,
    baseline_records_read: int,
) -> float:
    """Compute relative benefit against baseline with safe zero handling."""

    if baseline_records_read <= 0:
        return 0.0
    return 1.0 - (candidate_records_read / baseline_records_read)


def aggregate_layout_metrics(
    *,
    evaluation_id: str,
    candidate_key: str,
    partition_strategy: str,
    partition_columns: list[str],
    layout_type: str,
    layout_columns: list[str],
    query_estimates: list[QueryEstimate],
    total_records: int,
    total_bytes: int,
    total_row_groups: int,
    score_weights: ScoreWeights,
    max_layout_columns: int,
    algorithm: str,
    notes: str | None,
    include_query_estimates: bool,
) -> LayoutEvaluation:
    """Aggregate query-level metrics into one layout-level evaluation."""

    record_ratios = [
        estimate.estimated_records_read / max(total_records, 1)
        for estimate in query_estimates
    ]
    byte_ratios = [
        estimate.estimated_bytes_read / max(total_bytes, 1)
        for estimate in query_estimates
    ]
    row_group_ratios = [
        estimate.estimated_row_groups_read / max(total_row_groups, 1)
        for estimate in query_estimates
    ]
    benefit_coverage_30 = (
        sum(1 for estimate in query_estimates if estimate.benefit_vs_baseline >= 0.30)
        / max(len(query_estimates), 1)
    )
    avg_record_read_ratio = sum(record_ratios) / max(len(record_ratios), 1)
    avg_byte_read_ratio = sum(byte_ratios) / max(len(byte_ratios), 1)
    avg_row_group_read_ratio = sum(row_group_ratios) / max(len(row_group_ratios), 1)
    worst_query_read_ratio = max(record_ratios, default=0.0)
    num_layout_columns = len(layout_columns)
    layout_complexity = layout_complexity_for(layout_type)

    composite_score = compute_composite_score(
        avg_record_read_ratio=avg_record_read_ratio,
        benefit_coverage_30=benefit_coverage_30,
        worst_query_read_ratio=worst_query_read_ratio,
        layout_complexity=layout_complexity,
        num_layout_columns=num_layout_columns,
        max_layout_columns=max_layout_columns,
        weights=score_weights,
    )
    return LayoutEvaluation(
        evaluation_id=evaluation_id,
        candidate_key=candidate_key,
        partition_strategy=partition_strategy,
        partition_columns=list(partition_columns),
        layout_type=layout_type,
        layout_columns=list(layout_columns),
        num_partition_columns=len(partition_columns),
        num_layout_columns=num_layout_columns,
        layout_complexity=layout_complexity,
        query_estimates=query_estimates if include_query_estimates else [],
        avg_record_read_ratio=round(avg_record_read_ratio, 4),
        avg_byte_read_ratio=round(avg_byte_read_ratio, 4),
        avg_row_group_read_ratio=round(avg_row_group_read_ratio, 4),
        benefit_coverage_30=round(benefit_coverage_30, 4),
        worst_query_read_ratio=round(worst_query_read_ratio, 4),
        composite_score=round(composite_score, 4),
        algorithm=algorithm,
        notes=notes,
    )


def compute_composite_score(
    *,
    avg_record_read_ratio: float,
    benefit_coverage_30: float,
    worst_query_read_ratio: float,
    layout_complexity: int,
    num_layout_columns: int,
    max_layout_columns: int,
    weights: ScoreWeights,
) -> float:
    """Compute an optional utility-like composite score.

    Higher is better.
    - Rewards average read saving
    - Rewards workload coverage with material benefit
    - Rewards better worst-case pruning
    - Penalizes more complex layouts
    - Penalizes wider layouts with more columns
    - Penalizes the baseline no-layout case so it does not dominate by avoiding
      all structural penalties
    """

    avg_read_saving = 1.0 - avg_record_read_ratio
    worst_case_saving = 1.0 - worst_query_read_ratio
    complexity_penalty = layout_complexity / 3 if layout_complexity > 0 else 0.0
    column_penalty = num_layout_columns / max(max_layout_columns, 1)
    no_layout_penalty = 0.15 if layout_complexity == 0 and num_layout_columns == 0 else 0.0

    utility = (
        weights.read_saving_weight * avg_read_saving
        + weights.coverage_weight * benefit_coverage_30
        + weights.worst_case_penalty_weight * worst_case_saving
    )
    structural_penalty = (
        weights.layout_complexity_penalty_weight * complexity_penalty
        + weights.num_columns_penalty_weight * column_penalty
    )
    return max(0.0, min(1.0, utility - structural_penalty - no_layout_penalty))


def layout_complexity_for(layout_type: str) -> int:
    """Return a small explicit complexity scale for the current layout family."""

    return {
        "no layout": 0,
        "linear": 1,
        "zorder": 2,
        "hilbert": 3,
    }.get(layout_type, 0)


def _copy_query_estimates(estimates: list[QueryEstimate]) -> list[QueryEstimate]:
    return [estimate.model_copy() for estimate in estimates]


def _score_value(evaluation: LayoutEvaluation) -> float:
    return (
        evaluation.composite_score
        if evaluation.composite_score is not None
        else 1.0 - evaluation.avg_record_read_ratio
    )


def _build_evaluation_id(
    *,
    partition_strategy: str,
    partition_columns: list[str],
    layout_type: str,
    candidate_key: str,
) -> str:
    partition_part = (
        f"{partition_strategy}[{','.join(partition_columns)}]"
        if partition_strategy != "none" and partition_columns
        else "none[]"
    )
    return f"{partition_part}::{layout_type}::{candidate_key}"


def _deterministic_ratio(parts: list[str], *, low: float, high: float) -> float:
    signature = "|".join(parts)
    seed = int(hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)
    return rng.uniform(low, high)
