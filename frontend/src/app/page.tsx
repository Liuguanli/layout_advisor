"use client";

import { useCallback, useEffect, useState } from "react";

import DatasetUploadPanel from "../components/DatasetUploadPanel";
import LayoutPlaceholderPanel from "../components/LayoutPlaceholderPanel";
import RightSidebarNav from "../components/RightSidebarNav";
import VerificationPanel from "../components/VerificationPanel";
import WorkloadUploadPanel from "../components/WorkloadUploadPanel";
import {
  fetchDatasetCatalog,
  fetchWorkloadCatalog,
  fetchWorkloadSummary,
} from "../lib/api";
import {
  DatasetSummary,
  LayoutEvaluation,
  StaticDatasetItem,
  StaticWorkloadItem,
  WorkloadSummary,
  WorkloadUploadResponse,
} from "../lib/types";

export default function HomePage() {
  const navItems = [
    { id: "dataset-section", label: "Dataset", note: "catalog and profiles" },
    { id: "workload-section", label: "Workload", note: "catalog and analysis" },
    { id: "layout-section", label: "Physical Design", note: "partition, layout, future knobs" },
    { id: "verification-section", label: "Verification", note: "estimated vs actual" },
  ];
  const [datasetSummary, setDatasetSummary] = useState<DatasetSummary | null>(null);
  const [datasetOptions, setDatasetOptions] = useState<StaticDatasetItem[]>([]);
  const [workloadOptions, setWorkloadOptions] = useState<StaticWorkloadItem[]>([]);
  const [workloadSummary, setWorkloadSummary] = useState<WorkloadSummary | null>(null);
  const [workloadUploadResult, setWorkloadUploadResult] =
    useState<WorkloadUploadResponse | null>(null);
  const [warning, setWarning] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [comparisonList, setComparisonList] = useState<LayoutEvaluation[]>([]);
  const [globalLoadingCount, setGlobalLoadingCount] = useState(0);
  const [globalLoadingLabel, setGlobalLoadingLabel] = useState<string | null>(null);

  const beginGlobalLoading = useCallback((label: string) => {
    setGlobalLoadingLabel(label);
    setGlobalLoadingCount((current) => current + 1);
  }, []);

  const endGlobalLoading = useCallback(() => {
    setGlobalLoadingCount((current) => Math.max(0, current - 1));
  }, []);

  useEffect(() => {
    const loadExistingState = async () => {
      const issues: string[] = [];

      try {
        const catalog = await fetchDatasetCatalog();
        setDatasetOptions(catalog.datasets);
        if (catalog.datasets.length === 0) {
          issues.push("No datasets are configured in the backend catalog.");
        }
      } catch {
        issues.push("Dataset catalog could not be loaded from the backend.");
      }

      try {
        const catalog = await fetchWorkloadCatalog();
        setWorkloadOptions(catalog.workloads);
        if (catalog.workloads.length === 0) {
          issues.push("No workloads are configured in the backend catalog.");
        }
      } catch {
        issues.push("Workload catalog could not be loaded from the backend.");
      }

      setStatusMessage(issues.length > 0 ? issues.join(" ") : null);
    };

    void loadExistingState();
  }, []);

  const handleDatasetSelected = (summary: DatasetSummary | null) => {
    setDatasetSummary(summary);
    setWarning(null);
  };

  const handleWorkloadSelected = async (result: WorkloadUploadResponse | null) => {
    if (!result) {
      setWorkloadUploadResult(null);
      setWorkloadSummary(null);
      setWarning(null);
      return;
    }

    setWorkloadUploadResult(result);

    if (result.imported_queries === 0) {
      setWorkloadSummary(null);
      setWarning("No valid queries were loaded. Check workload file and try again.");
      return;
    }

    const summary = await fetchWorkloadSummary();
    setWorkloadSummary(summary);
    setWarning(
      result.failed_queries > 0
        ? `${result.failed_queries} query lines failed parsing and were skipped.`
        : null,
    );
  };

  return (
    <main className="page">
      {globalLoadingCount > 0 && (
        <div className="page-loading-overlay" role="status" aria-live="polite" aria-busy="true">
          <div className="page-loading-card">
            <div className="page-loading-spinner" aria-hidden="true" />
            <p>{globalLoadingLabel ?? "Loading"}</p>
          </div>
        </div>
      )}
      <header className="header">
        <h1>Layout Exploration System Prototype</h1>
      </header>

      {statusMessage && <p className="warning">{statusMessage}</p>}
      {warning && <p className="warning">{warning}</p>}

      <div className="page-shell">
        <div className="panel-stack">
          <section id="dataset-section" className="anchor-section">
            <DatasetUploadPanel
              datasetSummary={datasetSummary}
              datasetOptions={datasetOptions}
              onSelected={handleDatasetSelected}
              onGlobalLoadingStart={beginGlobalLoading}
              onGlobalLoadingEnd={endGlobalLoading}
            />
          </section>
          <section id="workload-section" className="anchor-section">
            <WorkloadUploadPanel
              uploadResult={workloadUploadResult}
              workloadOptions={workloadOptions}
              workloadSummary={workloadSummary}
              onSelected={handleWorkloadSelected}
              onGlobalLoadingStart={beginGlobalLoading}
              onGlobalLoadingEnd={endGlobalLoading}
            />
          </section>
          <section id="layout-section" className="anchor-section">
            <LayoutPlaceholderPanel
              columns={datasetSummary?.columns.map((column) => column.name) ?? []}
              datasetSummary={datasetSummary}
              workloadSummary={workloadSummary}
              onComparisonListChange={setComparisonList}
              onGlobalLoadingStart={beginGlobalLoading}
              onGlobalLoadingEnd={endGlobalLoading}
            />
          </section>
          <section id="verification-section" className="anchor-section">
            <VerificationPanel
              datasetSummary={datasetSummary}
              workloadSummary={workloadSummary}
              comparisonList={comparisonList}
            />
          </section>
        </div>
        <RightSidebarNav items={navItems} />
      </div>
    </main>
  );
}
