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

const CONFIGURED_API_BASE = process.env.NEXT_PUBLIC_API_BASE?.trim();

function getApiBase(): string {
  if (CONFIGURED_API_BASE) {
    return CONFIGURED_API_BASE;
  }

  if (typeof window !== "undefined") {
    return `${window.location.protocol}//${window.location.hostname}:8001`;
  }

  return "http://127.0.0.1:8001";
}

async function parseError(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { detail?: string };
    return body.detail ?? `Request failed with status ${response.status}`;
  } catch {
    return `Request failed with status ${response.status}`;
  }
}

export async function fetchDatasetCatalog(): Promise<DatasetCatalogResponse> {
  const response = await fetch(`${getApiBase()}/api/dataset/catalog`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as DatasetCatalogResponse;
}

export async function selectDataset(datasetId: string): Promise<DatasetSummary> {
  const response = await fetch(`${getApiBase()}/api/dataset/select`, {
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
  const response = await fetch(`${getApiBase()}/api/dataset/summary`, {
    method: "GET",
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(await parseError(response));
  }

  return (await response.json()) as DatasetSummary;
}

export async function fetchDatasetCorrelation(
  selectedColumns: string[] = [],
): Promise<DatasetSummary> {
  const response = await fetch(`${getApiBase()}/api/dataset/correlation`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ selected_columns: selectedColumns }),
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
  const response = await fetch(`${getApiBase()}/api/dataset/profile-sample`, {
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
  const response = await fetch(`${getApiBase()}/api/workload/catalog`, {
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
  const response = await fetch(`${getApiBase()}/api/workload/select`, {
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
  const response = await fetch(`${getApiBase()}/api/workload/summary`, {
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
  const response = await fetch(`${getApiBase()}/api/layout/estimate`, {
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
  const response = await fetch(`${getApiBase()}/api/layout/evaluate`, {
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
  const response = await fetch(`${getApiBase()}/api/layout/mock-execute`, {
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
