"""Models for dataset summary and static catalog responses."""

from pydantic import BaseModel, Field, model_validator


class ColumnInfo(BaseModel):
    """Single column metadata inferred from uploaded dataset."""

    name: str
    inferred_type: str


class DistributionBucket(BaseModel):
    """Single bucket in a histogram-like column distribution."""

    label: str
    count: int = Field(..., ge=0)


class ColumnProfile(BaseModel):
    """Observed profile for a column from sampled dataset rows."""

    name: str
    inferred_type: str
    sample_size: int = Field(..., ge=0)
    null_count: int = Field(..., ge=0)
    distinct_count: int = Field(..., ge=0)
    min_value: str | None = None
    max_value: str | None = None
    distribution_kind: str
    distribution: list[DistributionBucket]


class CorrelationPair(BaseModel):
    """One correlated column pair with exact Pearson score."""

    column_a: str
    column_b: str
    correlation: float
    observation_count: int = Field(..., ge=0)


class CorrelationSummary(BaseModel):
    """Column association matrix summary across supported column types."""

    method: str
    mode: str
    columns: list[str]
    column_kinds: dict[str, str] = Field(default_factory=dict)
    matrix: list[list[float | None]]
    top_pairs: list[CorrelationPair]

    @model_validator(mode="before")
    @classmethod
    def backfill_legacy_column_kinds(cls, data: object) -> object:
        """Backfill `column_kinds` for legacy Pearson-only payloads."""

        if not isinstance(data, dict):
            return data

        if "column_kinds" not in data or data["column_kinds"] is None:
            columns = data.get("columns", [])
            data["column_kinds"] = {
                str(column): "ordered" for column in columns if isinstance(column, str)
            }
        return data


class DatasetSummary(BaseModel):
    """Summary statistics for the currently uploaded dataset."""

    dataset_id: str | None = None
    dataset_name: str | None = None
    row_count: int = Field(..., ge=0)
    profile_sample_size: int = Field(..., ge=0)
    columns: list[ColumnInfo]
    column_profiles: list[ColumnProfile]
    correlation_summary: CorrelationSummary | None = None


class StaticDatasetItem(BaseModel):
    """Static dataset entry exposed to the frontend."""

    dataset_id: str
    name: str
    file_path: str


class DatasetCatalogResponse(BaseModel):
    """List of available static datasets."""

    datasets: list[StaticDatasetItem]


class SelectDatasetRequest(BaseModel):
    """Request payload for selecting a configured static dataset."""

    dataset_id: str


class UpdateDatasetSampleRequest(BaseModel):
    """Request payload for updating sampled profile size."""

    sample_size: int = Field(..., ge=1)
