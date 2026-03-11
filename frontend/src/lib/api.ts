import {
  DatasetCatalogResponse,
  LayoutEstimateRequest,
  LayoutEstimateResponse,
  LayoutEvaluationRequest,
  LayoutEvaluationResponse,
  MockExecutionRequest,
  MockExecutionResponse,
  DatasetSummary,
  WorkloadCatalogResponse,
  WorkloadSummary,
  WorkloadUploadResponse,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8001";

async function parseError(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { detail?: string };
    return body.detail ?? `Request failed with status ${response.status}`;
  } catch {
    return `Request failed with status ${response.status}`;
  }
}

export async function fetchDatasetCatalog(): Promise<DatasetCatalogResponse> {
  const response = await fetch(`${API_BASE}/api/dataset/catalog`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as DatasetCatalogResponse;
}

export async function selectDataset(datasetId: string): Promise<DatasetSummary> {
  const response = await fetch(`${API_BASE}/api/dataset/select`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ dataset_id: datasetId }),
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as DatasetSummary;
}

export async function fetchDatasetSummary(): Promise<DatasetSummary> {
  const response = await fetch(`${API_BASE}/api/dataset/summary`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as DatasetSummary;
}

export async function fetchDatasetCorrelation(): Promise<DatasetSummary> {
  const response = await fetch(`${API_BASE}/api/dataset/correlation`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as DatasetSummary;
}

export async function updateDatasetProfileSample(
  sampleSize: number,
): Promise<DatasetSummary> {
  const response = await fetch(`${API_BASE}/api/dataset/profile-sample`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ sample_size: sampleSize }),
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as DatasetSummary;
}

export async function fetchWorkloadCatalog(): Promise<WorkloadCatalogResponse> {
  const response = await fetch(`${API_BASE}/api/workload/catalog`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as WorkloadCatalogResponse;
}

export async function selectWorkload(
  workloadId: string,
): Promise<WorkloadUploadResponse> {
  const response = await fetch(`${API_BASE}/api/workload/select`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ workload_id: workloadId }),
  });


  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as WorkloadUploadResponse;
}

export async function fetchWorkloadSummary(): Promise<WorkloadSummary> {
  const response = await fetch(`${API_BASE}/api/workload/summary`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as WorkloadSummary;
}

export async function estimateLayout(
  payload: LayoutEstimateRequest,
): Promise<LayoutEstimateResponse> {
  const response = await fetch(`${API_BASE}/api/layout/estimate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as LayoutEstimateResponse;
}

export async function evaluateLayout(
  payload: LayoutEvaluationRequest,
): Promise<LayoutEvaluationResponse> {
  const response = await fetch(`${API_BASE}/api/layout/evaluate`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as LayoutEvaluationResponse;
}

export async function runMockLayoutExecution(
  payload: MockExecutionRequest,
): Promise<MockExecutionResponse> {
  const response = await fetch(`${API_BASE}/api/layout/mock-execute`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as MockExecutionResponse;
}
