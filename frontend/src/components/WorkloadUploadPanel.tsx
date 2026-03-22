"use client";

import { FormEvent, ReactNode, useEffect, useMemo, useState } from "react";

import { selectWorkload } from "../lib/api";
import { StaticWorkloadItem, WorkloadSummary, WorkloadUploadResponse } from "../lib/types";
import CollapsibleSubsection from "./CollapsibleSubsection";
import PanelHeader from "./PanelHeader";
import WorkloadDashboard from "./WorkloadDashboard";

type WorkloadUploadPanelProps = {
  uploadResult: WorkloadUploadResponse | null;
  workloadOptions: StaticWorkloadItem[];
  workloadSummary: WorkloadSummary | null;
  onSelected: (result: WorkloadUploadResponse | null) => Promise<void>;
  onGlobalLoadingStart?: (label: string) => void;
  onGlobalLoadingEnd?: () => void;
  headerAction?: ReactNode;
};

export default function WorkloadUploadPanel({
  uploadResult,
  workloadOptions,
  workloadSummary,
  onSelected,
  onGlobalLoadingStart,
  onGlobalLoadingEnd,
  headerAction,
}: WorkloadUploadPanelProps) {
  const defaultId = useMemo(
    () => "",
    [workloadOptions],
  );
  const [workloadId, setWorkloadId] = useState(defaultId);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!workloadId && defaultId) {
      setWorkloadId(defaultId);
    }
  }, [workloadId, defaultId]);

  useEffect(() => {
    if (uploadResult) {
      return;
    }
    setWorkloadId("");
  }, [uploadResult]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!workloadId) {
      setError(null);
      await onSelected(null);
      return;
    }

    setLoading(true);
    setError(null);
    onGlobalLoadingStart?.("Loading workload");

    try {
      const result = await selectWorkload(workloadId);
      await onSelected(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Workload selection failed.");
    } finally {
      setLoading(false);
      onGlobalLoadingEnd?.();
    }
  };

  return (
    <section className="panel">
      <PanelHeader
        title="2. Query Workload Selection (Static Catalog)"
        action={headerAction}
      />

      <form onSubmit={handleSubmit} className="panel-form">
        <select
          value={workloadId}
          onChange={(event) => {
            const nextValue = event.target.value;
            setWorkloadId(nextValue);
            setError(null);
            if (!nextValue) {
              void onSelected(null);
            }
          }}
        >
          {workloadOptions.length === 0 ? (
            <option value="">No workload configured</option>
          ) : (
            <>
              <option value="">No workload selected</option>
              {workloadOptions.map((workload) => (
                <option key={workload.workload_id} value={workload.workload_id}>
                  {workload.name}
                </option>
              ))}
            </>
          )}
        </select>
        <button
          type="submit"
          disabled={loading || workloadOptions.length === 0 || !workloadId}
        >
          Load Workload
        </button>
      </form>

      {error && <p className="error">{error}</p>}

      {uploadResult && (
        <CollapsibleSubsection title="Import Summary">
          <p style={{ marginTop: 0 }}>
            <strong>Imported queries:</strong> {uploadResult.imported_queries}
          </p>
          <p>
            <strong>Failed queries:</strong> {uploadResult.failed_queries}
          </p>
        </CollapsibleSubsection>
      )}

      <WorkloadDashboard summary={workloadSummary} embedded />
    </section>
  );
}
