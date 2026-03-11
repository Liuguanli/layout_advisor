"""Shared in-memory singleton services."""

from app.services.dataset_service import DatasetService
from app.services.layout_service import LayoutService
from app.services.workload_service import WorkloadService


dataset_service = DatasetService()
workload_service = WorkloadService()
layout_service = LayoutService(dataset_service, workload_service)
