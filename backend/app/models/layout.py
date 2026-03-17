"""Models for layout scoring, metrics aggregation, and placeholder estimation."""

from pydantic import BaseModel, Field


class LayoutPermutationCandidate(BaseModel):
    """One selected permutation candidate from the frontend."""

    key: str
    columns: list[str]


class LayoutEstimateRequest(BaseModel):
    """Request payload for placeholder layout estimation."""

    dataset_id: str | None = None
    partition_strategy: str = "none"
    partition_columns: list[str] = Field(default_factory=list)
    layout_types: list[str]
    selected_candidates: list[LayoutPermutationCandidate]


class LayoutEstimateItem(BaseModel):
    """One estimated layout candidate with placeholder cost."""

    estimate_id: str
    layout_type: str
    candidate_key: str
    column_order: list[str]
    estimated_cost: float = Field(..., ge=0)
    algorithm: str
    notes: str | None = None


class LayoutEstimateResponse(BaseModel):
    """Batch estimation result for selected layout candidates."""

    dataset_id: str | None = None
    workload_loaded: bool
    total_estimates: int = Field(..., ge=0)
    estimates: list[LayoutEstimateItem]


class QueryEstimate(BaseModel):
    """Estimated read metrics for one query under one candidate layout."""

    query_id: str
    predicate_columns: list[str] = Field(default_factory=list)
    estimated_records_read: int = Field(..., ge=0)
    estimated_bytes_read: int = Field(..., ge=0)
    estimated_row_groups_read: int = Field(..., ge=0)
    benefit_vs_baseline: float = 0.0


class ScoreWeights(BaseModel):
    """Configurable weights for the optional composite layout score.

    The composite score is utility-like: higher is better.
    It rewards read saving, broader benefit coverage, and better worst-case
    pruning while penalizing layout complexity and wider layout keys.
    """

    read_saving_weight: float = Field(default=0.45, ge=0)
    coverage_weight: float = Field(default=0.2, ge=0)
    worst_case_penalty_weight: float = Field(default=0.2, ge=0)
    layout_complexity_penalty_weight: float = Field(default=0.1, ge=0)
    num_columns_penalty_weight: float = Field(default=0.05, ge=0)


class LayoutEvaluation(BaseModel):
    """Aggregated workload-level metrics for one candidate layout."""

    evaluation_id: str
    candidate_key: str
    partition_strategy: str = "none"
    partition_columns: list[str] = Field(default_factory=list)
    layout_type: str
    layout_columns: list[str]
    num_partition_columns: int = Field(default=0, ge=0)
    num_layout_columns: int = Field(..., ge=0)
    layout_complexity: int = Field(..., ge=0)
    query_estimates: list[QueryEstimate] = Field(default_factory=list)
    avg_record_read_ratio: float = Field(..., ge=0)
    avg_byte_read_ratio: float = Field(..., ge=0)
    avg_row_group_read_ratio: float = Field(..., ge=0)
    benefit_coverage_30: float = Field(..., ge=0)
    worst_query_read_ratio: float = Field(..., ge=0)
    composite_score: float | None = Field(default=None, ge=0)
    algorithm: str
    notes: str | None = None


class LayoutEvaluationRequest(BaseModel):
    """Request payload for workload-aware layout evaluation."""

    dataset_id: str | None = None
    partition_strategy: str = "none"
    partition_columns: list[str] = Field(default_factory=list)
    layout_types: list[str]
    selected_candidates: list[LayoutPermutationCandidate]
    score_weights: ScoreWeights | None = None
    include_query_estimates: bool = False


class LayoutEvaluationResponse(BaseModel):
    """Workload-aware evaluation result for the selected layout candidates."""

    dataset_id: str | None = None
    workload_loaded: bool
    total_queries: int = Field(..., ge=0)
    total_records: int = Field(..., ge=0)
    total_bytes: int = Field(..., ge=0)
    total_row_groups: int = Field(..., ge=0)
    sample_ratio: float = Field(..., ge=0)
    score_weights: ScoreWeights | None = None
    evaluations: list[LayoutEvaluation]


class MockExecutionCandidate(BaseModel):
    """Minimal candidate payload for a mock actual benchmark run."""

    evaluation_id: str
    partition_strategy: str = "none"
    partition_columns: list[str] = Field(default_factory=list)
    layout_type: str
    layout_columns: list[str]
    estimated_score: float = Field(..., ge=0)
    avg_record_read_ratio: float = Field(..., ge=0)
    avg_row_group_read_ratio: float = Field(..., ge=0)
    layout_complexity: int = Field(..., ge=0)


class MockExecutionRequest(BaseModel):
    """Request payload for deterministic mock actual execution."""

    dataset_id: str | None = None
    candidates: list[MockExecutionCandidate]


class MockExecutionResult(BaseModel):
    """Mock actual benchmark result for one evaluated layout candidate."""

    evaluation_id: str
    partition_strategy: str = "none"
    partition_columns: list[str] = Field(default_factory=list)
    layout_type: str
    layout_columns: list[str]
    actual_runtime_ms: float = Field(..., ge=0)
    actual_records_read_ratio: float = Field(..., ge=0)
    actual_row_group_read_ratio: float = Field(..., ge=0)
    actual_score: float = Field(..., ge=0)
    runner: str
    notes: str | None = None


class MockExecutionResponse(BaseModel):
    """Batch mock actual benchmark results."""

    dataset_id: str | None = None
    total_results: int = Field(..., ge=0)
    results: list[MockExecutionResult]
