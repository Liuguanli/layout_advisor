"""Layout estimation and evaluation API endpoints."""

from fastapi import APIRouter, HTTPException

from app.models.layout import (
    LayoutEstimateRequest,
    LayoutEstimateResponse,
    LayoutEvaluationRequest,
    LayoutEvaluationResponse,
    MockExecutionRequest,
    MockExecutionResponse,
)
from app.services.state import layout_service

router = APIRouter(prefix="/api/layout", tags=["layout"])


@router.post("/estimate", response_model=LayoutEstimateResponse)
def estimate_layout(payload: LayoutEstimateRequest) -> LayoutEstimateResponse:
    """Run placeholder layout estimation for selected candidates."""

    try:
        return layout_service.estimate(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/evaluate", response_model=LayoutEvaluationResponse)
def evaluate_layout(payload: LayoutEvaluationRequest) -> LayoutEvaluationResponse:
    """Run workload-aware layout evaluation with aggregated metrics."""

    try:
        return layout_service.evaluate(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/mock-execute", response_model=MockExecutionResponse)
def mock_execute_layout(payload: MockExecutionRequest) -> MockExecutionResponse:
    """Run a deterministic mock actual benchmark for selected candidates."""

    try:
        return layout_service.mock_execute(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
