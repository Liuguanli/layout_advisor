"""FastAPI entrypoint for layout exploration prototype backend."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.dataset import router as dataset_router
from app.api.routes.layout import router as layout_router
from app.api.routes.workload import router as workload_router

app = FastAPI(
    title="Layout Exploration Prototype API",
    version="0.1.0",
    description="Dataset and workload ingestion service for early-stage layout exploration.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_origin_regex=r"^https?://((localhost|127\.0\.0\.1)|((10|192\.168)\.\d{1,3}\.\d{1,3})|(172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3}))(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dataset_router)
app.include_router(workload_router)
app.include_router(layout_router)


@app.get("/")
def root() -> dict[str, str]:
    """Simple root endpoint to confirm the API is running."""

    return {"message": "Layout Exploration Prototype API"}


@app.get("/api/health")
def health() -> dict[str, str]:
    """Basic liveness endpoint for local development."""

    return {"status": "ok"}
