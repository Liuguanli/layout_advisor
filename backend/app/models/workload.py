"""Models for workload catalog, ingestion and analysis responses."""

from pydantic import BaseModel, Field


class StaticWorkloadItem(BaseModel):
    """Static workload entry exposed to the frontend."""

    workload_id: str
    name: str
    file_path: str


class WorkloadCatalogResponse(BaseModel):
    """List of available static workloads."""

    workloads: list[StaticWorkloadItem]


class SelectWorkloadRequest(BaseModel):
    """Request payload for selecting a configured static workload."""

    workload_id: str


class WorkloadUploadResponse(BaseModel):
    """Result of workload file ingestion."""

    imported_queries: int = Field(..., ge=0)
    failed_queries: int = Field(..., ge=0)


class PairFrequency(BaseModel):
    """Co-occurrence frequency for a pair of filter columns."""

    column_a: str
    column_b: str
    count: int = Field(..., ge=0)


class PredicateCombinationFrequency(BaseModel):
    """Frequency for an exact predicate-column combination in a query."""

    columns: list[str]
    count: int = Field(..., ge=0)


class WorkloadSummary(BaseModel):
    """Aggregate metrics extracted from parsed SQL workload."""

    total_queries: int = Field(..., ge=0)
    predicate_type_distribution: dict[str, int]
    per_column_filter_frequency: dict[str, int]
    per_column_predicate_type_distribution: dict[str, dict[str, int]] = Field(default_factory=dict)
    per_column_avg_predicate_selectivity: dict[str, float] = Field(default_factory=dict)
    per_column_avg_query_selectivity: dict[str, float] = Field(default_factory=dict)
    top_predicate_combinations: list[PredicateCombinationFrequency]
    top_cooccurring_filter_pairs: list[PairFrequency]
    query_complexity_distribution: dict[str, int]
