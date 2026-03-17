"use client";

import { useCallback, useEffect, useState } from "react";

import DatasetUploadPanel from "../components/DatasetUploadPanel";
import LayoutPlaceholderPanel from "../components/LayoutPlaceholderPanel";
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

type TopTabId = "dataset" | "workload" | "layout" | "verification";
type TopTab = {
  id: TopTabId;
  label: string;
  note: string;
};

export default function HomePage() {
  const tabs: TopTab[] = [
    { id: "dataset", label: "Dataset", note: "Load data" },
    { id: "workload", label: "Workload", note: "Load queries" },
    { id: "layout", label: "Physical Design", note: "Choose design" },
    { id: "verification", label: "Verification", note: "Validate ranking" },
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
  const [activeTab, setActiveTab] = useState<TopTabId>("dataset");
  const [verificationComplete, setVerificationComplete] = useState(false);

  const datasetComplete = Boolean(datasetSummary?.dataset_id);
  const workloadComplete = Boolean(workloadSummary && workloadSummary.total_queries > 0);
  const layoutComplete = comparisonList.length > 0;
  const verificationReady = layoutComplete;

  const tabEnabled: Record<TopTabId, boolean> = {
    dataset: true,
    workload: datasetComplete,
    layout: workloadComplete,
    verification: verificationReady,
  };

  const tabComplete: Record<TopTabId, boolean> = {
    dataset: datasetComplete,
    workload: workloadComplete,
    layout: layoutComplete,
    verification: verificationComplete,
  };

  const activeTabIndex = tabs.findIndex((tab) => tab.id === activeTab);
  const nextTab = activeTabIndex >= 0 ? tabs[activeTabIndex + 1] ?? null : null;
  const canAdvanceToNext = Boolean(nextTab && tabEnabled[nextTab.id]);
  const nextStepButton = (
    <button
      type="button"
      className="page-shell-next-button"
      onClick={() => {
        if (nextTab && tabEnabled[nextTab.id]) {
          setActiveTab(nextTab.id);
        }
      }}
      disabled={!canAdvanceToNext}
    >
      Next Step
    </button>
  );

  useEffect(() => {
    const nextTab = verificationReady
      ? "verification"
      : workloadComplete
        ? "layout"
        : datasetComplete
          ? "workload"
          : "dataset";

    if (!tabEnabled[activeTab]) {
      setActiveTab(nextTab);
    }
  }, [activeTab, datasetComplete, verificationReady, workloadComplete]);

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
    setComparisonList([]);
    setVerificationComplete(false);
    setWarning(null);
  };

  const handleWorkloadSelected = async (result: WorkloadUploadResponse | null) => {
    if (!result) {
      setWorkloadUploadResult(null);
      setWorkloadSummary(null);
      setComparisonList([]);
      setVerificationComplete(false);
      setWarning(null);
      return;
    }

    setWorkloadUploadResult(result);

    if (result.imported_queries === 0) {
      setWorkloadSummary(null);
      setComparisonList([]);
      setVerificationComplete(false);
      setWarning("No valid queries were loaded. Check workload file and try again.");
      return;
    }

    const summary = await fetchWorkloadSummary();
    setWorkloadSummary(summary);
    setComparisonList([]);
    setVerificationComplete(false);
    setWarning(
      result.failed_queries > 0
        ? `${result.failed_queries} query lines failed parsing and were skipped.`
        : null,
    );
  };

  return (
    <main className="page">
      <header className="header">
        <h1>Layout Exploration System Prototype</h1>
      </header>

      {statusMessage && <p className="warning">{statusMessage}</p>}
      {warning && <p className="warning">{warning}</p>}

      <nav className="top-tabs" aria-label="Primary sections">
        <div className="top-tabs-list" role="tablist" aria-orientation="horizontal">
          {tabs.map((tab, index) => {
            const enabled = tabEnabled[tab.id];
            const complete = tabComplete[tab.id];
            const isActive = activeTab === tab.id;
            const statusLabel = complete
              ? "Completed step"
              : isActive
                ? "In progress"
                : enabled
                  ? "Ready step"
                  : "Unavailable step";

            return (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={isActive}
                aria-controls={`${tab.id}-panel`}
                id={`${tab.id}-tab`}
                aria-label={`${tab.label}. ${statusLabel}`}
                disabled={!enabled}
                className={`top-tab-card ${index === 0 ? "is-first" : ""} ${index === tabs.length - 1 ? "is-last" : ""} ${isActive ? "is-active" : ""} ${complete ? "is-complete" : ""} ${!enabled ? "is-locked" : ""}`}
                onClick={() => {
                  if (enabled) {
                    setActiveTab(tab.id);
                  }
                }}
              >
                <span
                  className={`top-tab-marker ${complete ? "is-complete" : isActive ? "is-active" : enabled ? "is-ready" : "is-locked"}`}
                  aria-hidden="true"
                >
                  <span className="top-tab-dot" />
                </span>
                <div className="top-tab-copy">
                  <strong>{tab.label}</strong>
                  <small>{tab.note}</small>
                </div>
              </button>
            );
          })}
        </div>
      </nav>

      <div className="page-shell">
        {globalLoadingCount > 0 && (
          <div className="page-loading-overlay" role="status" aria-live="polite" aria-busy="true">
            <div className="page-loading-card">
              <div className="page-loading-spinner" aria-hidden="true" />
              <p>{globalLoadingLabel ?? "Loading"}</p>
            </div>
          </div>
        )}
        <div className="panel-stack">
          <section
            id="dataset-panel"
            role="tabpanel"
            aria-labelledby="dataset-tab"
            className={`tab-panel ${activeTab === "dataset" ? "is-active" : ""}`}
            hidden={activeTab !== "dataset"}
          >
            <DatasetUploadPanel
              datasetSummary={datasetSummary}
              datasetOptions={datasetOptions}
              onSelected={handleDatasetSelected}
              onGlobalLoadingStart={beginGlobalLoading}
              onGlobalLoadingEnd={endGlobalLoading}
              headerAction={nextStepButton}
            />
          </section>
          <section
            id="workload-panel"
            role="tabpanel"
            aria-labelledby="workload-tab"
            className={`tab-panel ${activeTab === "workload" ? "is-active" : ""}`}
            hidden={activeTab !== "workload"}
          >
            <WorkloadUploadPanel
              uploadResult={workloadUploadResult}
              workloadOptions={workloadOptions}
              workloadSummary={workloadSummary}
              onSelected={handleWorkloadSelected}
              onGlobalLoadingStart={beginGlobalLoading}
              onGlobalLoadingEnd={endGlobalLoading}
              headerAction={nextStepButton}
            />
          </section>
          <section
            id="layout-panel"
            role="tabpanel"
            aria-labelledby="layout-tab"
            className={`tab-panel ${activeTab === "layout" ? "is-active" : ""}`}
            hidden={activeTab !== "layout"}
          >
            <LayoutPlaceholderPanel
              columns={datasetSummary?.columns.map((column) => column.name) ?? []}
              datasetSummary={datasetSummary}
              workloadSummary={workloadSummary}
              onComparisonListChange={setComparisonList}
              onGlobalLoadingStart={beginGlobalLoading}
              onGlobalLoadingEnd={endGlobalLoading}
              headerAction={nextStepButton}
            />
          </section>
          <section
            id="verification-panel"
            role="tabpanel"
            aria-labelledby="verification-tab"
            className={`tab-panel ${activeTab === "verification" ? "is-active" : ""}`}
            hidden={activeTab !== "verification"}
          >
            <VerificationPanel
              datasetSummary={datasetSummary}
              workloadSummary={workloadSummary}
              comparisonList={comparisonList}
              onStatusChange={setVerificationComplete}
              headerAction={nextStepButton}
            />
          </section>
        </div>
      </div>
    </main>
  );
}
