"""Dataset API endpoints."""

from fastapi import APIRouter, HTTPException

from app.models.dataset import (
    DatasetCatalogResponse,
    DatasetSummary,
    SelectDatasetRequest,
    UpdateDatasetSampleRequest,
)
from app.services.state import dataset_service

router = APIRouter(prefix="/api/dataset", tags=["dataset"])


@router.get("/catalog", response_model=DatasetCatalogResponse)
def get_dataset_catalog() -> DatasetCatalogResponse:
    """Return available static dataset entries."""

    try:
        return dataset_service.list_datasets()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/select", response_model=DatasetSummary)
def select_dataset(payload: SelectDatasetRequest) -> DatasetSummary:
    """Load dataset summary from a configured static dataset entry."""

    try:
        return dataset_service.select_dataset(payload.dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/summary", response_model=DatasetSummary)
def get_dataset_summary() -> DatasetSummary:
    """Get summary of the latest selected dataset."""

    try:
        return dataset_service.get_summary()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/correlation", response_model=DatasetSummary)
def get_dataset_correlation() -> DatasetSummary:
    """Compute and return correlation summary for the current dataset."""

    try:
        summary = dataset_service.get_summary()
        summary.correlation_summary = dataset_service.get_correlation_summary()
        return summary
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/profile-sample", response_model=DatasetSummary)
def update_dataset_profile_sample(
    payload: UpdateDatasetSampleRequest,
) -> DatasetSummary:
    """Recompute sampled dataset profiles with a new sample size."""

    try:
        return dataset_service.update_profile_sample(payload.sample_size)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
