"""Workload API endpoints."""

from fastapi import APIRouter, HTTPException

from app.models.workload import (
    SelectWorkloadRequest,
    WorkloadCatalogResponse,
    WorkloadSummary,
    WorkloadUploadResponse,
)
from app.services.state import dataset_service, workload_service

router = APIRouter(prefix="/api/workload", tags=["workload"])


@router.get("/catalog", response_model=WorkloadCatalogResponse)
def get_workload_catalog() -> WorkloadCatalogResponse:
    """Return available static workload entries."""

    try:
        return workload_service.list_workloads()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/select", response_model=WorkloadUploadResponse)
def select_workload(payload: SelectWorkloadRequest) -> WorkloadUploadResponse:
    """Select and parse a workload from the static catalog."""

    try:
        return workload_service.select_workload(payload.workload_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive fallback
        raise HTTPException(status_code=500, detail=f"Workload selection failed: {exc}") from exc


@router.get("/summary", response_model=WorkloadSummary)
def get_workload_summary() -> WorkloadSummary:
    """Get analysis metrics for the latest parsed workload."""

    try:
        try:
            dataset_summary = dataset_service.get_summary()
            sample_frame = dataset_service.get_sample_frame(dataset_summary.profile_sample_size)
            column_types = {
                column.name: column.inferred_type for column in dataset_summary.columns
            }
        except ValueError:
            sample_frame = None
            column_types = None

        return workload_service.summarize(
            sample_frame=sample_frame,
            column_types=column_types,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
