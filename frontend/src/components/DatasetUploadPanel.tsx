"use client";

import { FormEvent, ReactNode, useEffect, useMemo, useRef, useState } from "react";

import {
  fetchDatasetCorrelation,
  selectDataset,
  updateDatasetProfileSample,
} from "../lib/api";
import { DatasetSummary, StaticDatasetItem } from "../lib/types";
import CollapsibleHeader from "./CollapsibleHeader";
import CollapsibleSubsection from "./CollapsibleSubsection";
import DatasetCorrelationPanel from "./DatasetCorrelationPanel";
import DatasetProfilesPanel from "./DatasetProfilesPanel";

const DISTINCT_RATIO_DECIMALS = 3;

function formatDistinctRatio(value: number): string {
  return `${value.toFixed(DISTINCT_RATIO_DECIMALS)}%`;
}

type DatasetUploadPanelProps = {
  datasetSummary: DatasetSummary | null;
  datasetOptions: StaticDatasetItem[];
  onSelected: (summary: DatasetSummary | null) => void;
  onGlobalLoadingStart?: (label: string) => void;
  onGlobalLoadingEnd?: () => void;
  headerAction?: ReactNode;
};

export default function DatasetUploadPanel({
  datasetSummary,
  datasetOptions,
  onSelected,
  onGlobalLoadingStart,
  onGlobalLoadingEnd,
  headerAction,
}: DatasetUploadPanelProps) {
  const defaultId = useMemo(
    () => "",
    [datasetOptions],
  );
  const [datasetId, setDatasetId] = useState(defaultId);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const [showFullSchema, setShowFullSchema] = useState(false);
  const [sampleInput, setSampleInput] = useState("");
  const [sampleLoading, setSampleLoading] = useState(false);
  const [sampleError, setSampleError] = useState<string | null>(null);
  const [correlationSummary, setCorrelationSummary] = useState(
    datasetSummary?.correlation_summary ?? null,
  );
  const [correlationLoading, setCorrelationLoading] = useState(false);
  const [correlationError, setCorrelationError] = useState<string | null>(null);
  const [selectedProfileColumns, setSelectedProfileColumns] = useState<string[]>([]);
  const lastDatasetIdRef = useRef<string | null>(null);
  const profileSampleSize = datasetSummary?.profile_sample_size ?? datasetSummary?.row_count ?? 0;
  const rowCount = datasetSummary?.row_count ?? 0;
  const columnProfiles = useMemo(
    () => datasetSummary?.column_profiles ?? [],
    [datasetSummary?.column_profiles],
  );
  const datasetColumns = useMemo(
    () => datasetSummary?.columns ?? [],
    [datasetSummary?.columns],
  );
  const datasetColumnNames = useMemo(
    () => datasetColumns.map((column) => column.name),
    [datasetColumns],
  );
  const datasetColumnKey = useMemo(
    () => datasetColumnNames.join("\u0001"),
    [datasetColumnNames],
  );
  const profileByName = useMemo(
    () => new Map(columnProfiles.map((profile) => [profile.name, profile])),
    [columnProfiles],
  );
  const selectedProfileSet = useMemo(
    () => new Set(selectedProfileColumns),
    [selectedProfileColumns],
  );
  const visibleProfiles = useMemo(
    () => columnProfiles.filter((profile) => selectedProfileSet.has(profile.name)),
    [columnProfiles, selectedProfileSet],
  );

  useEffect(() => {
    if (!datasetId && defaultId) {
      setDatasetId(defaultId);
    }
  }, [datasetId, defaultId]);

  useEffect(() => {
    if (datasetSummary?.dataset_id) {
      setDatasetId(datasetSummary.dataset_id);
      return;
    }
    setDatasetId("");
  }, [datasetSummary?.dataset_id]);

  useEffect(() => {
    if (!datasetSummary) {
      setSampleInput("");
      return;
    }

    setSampleInput(String(datasetSummary.profile_sample_size));
    setSampleError(null);
  }, [datasetSummary?.profile_sample_size, datasetSummary?.row_count]);

  useEffect(() => {
    setCorrelationSummary(datasetSummary?.correlation_summary ?? null);
    setCorrelationError(null);
    setCorrelationLoading(false);
  }, [datasetSummary?.dataset_id]);

  useEffect(() => {
    const nextDatasetId = datasetSummary?.dataset_id ?? null;
    if (!nextDatasetId) {
      lastDatasetIdRef.current = null;
      setSelectedProfileColumns((current) => (current.length === 0 ? current : []));
      return;
    }
    if (lastDatasetIdRef.current === nextDatasetId) {
      return;
    }
    lastDatasetIdRef.current = nextDatasetId;
    setSelectedProfileColumns((current) => (current.length === 0 ? current : []));
  }, [datasetSummary?.dataset_id, datasetColumnKey]);

  const parsedSampleInput = Number.parseInt(sampleInput, 10);
  const requestedSampleSize = Number.isFinite(parsedSampleInput)
    ? Math.min(Math.max(parsedSampleInput, 1), Math.max(rowCount, 1))
    : null;
  const sampleRatio = rowCount > 0 && requestedSampleSize !== null
    ? (requestedSampleSize / rowCount) * 100
    : 0;
  const sliderRatio = Math.min(sampleRatio, 5);
  const hasPendingSampleChange = requestedSampleSize !== null
    && datasetSummary !== null
    && requestedSampleSize !== datasetSummary.profile_sample_size;

  const handleApplySample = async () => {
    if (!datasetSummary) {
      return;
    }

    if (requestedSampleSize === null) {
      setSampleError("Sample rows must be a valid integer.");
      return;
    }

    if (!hasPendingSampleChange) {
      return;
    }

    setSampleLoading(true);
    setSampleError(null);
    onGlobalLoadingStart?.("Refreshing column profiles");

    try {
      const summary = await updateDatasetProfileSample(requestedSampleSize);
      onSelected(summary);
    } catch (err) {
      setSampleError(
        err instanceof Error ? err.message : "Profile sample update failed.",
      );
    } finally {
      setSampleLoading(false);
      onGlobalLoadingEnd?.();
    }
  };

  const handleResetSample = () => {
    if (!datasetSummary) {
      return;
    }

    setSampleInput(String(datasetSummary.profile_sample_size));
    setSampleError(null);
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!datasetId) {
      setError(null);
      onSelected(null);
      return;
    }

    setLoading(true);
    setError(null);
    onGlobalLoadingStart?.("Loading dataset");

    try {
      const summary = await selectDataset(datasetId);
      onSelected(summary);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Dataset selection failed.");
    } finally {
      setLoading(false);
      onGlobalLoadingEnd?.();
    }
  };

  const handleLoadCorrelation = async () => {
    setCorrelationLoading(true);
    setCorrelationError(null);
    onGlobalLoadingStart?.("Computing correlation matrix");

    try {
      const summary = await fetchDatasetCorrelation();
      setCorrelationSummary(summary.correlation_summary);
      onSelected(summary);
    } catch (err) {
      setCorrelationError(
        err instanceof Error ? err.message : "Correlation loading failed.",
      );
    } finally {
      setCorrelationLoading(false);
      onGlobalLoadingEnd?.();
    }
  };

  const toggleProfileColumn = (columnName: string) => {
    setSelectedProfileColumns((current) =>
      current.includes(columnName)
        ? current.filter((column) => column !== columnName)
        : [...current, columnName],
    );
  };

  const selectAllProfileColumns = () => {
    setSelectedProfileColumns(datasetColumns.map((column) => column.name));
  };

  const clearProfileColumns = () => {
    setSelectedProfileColumns([]);
  };

  return (
    <section className="panel">
      <CollapsibleHeader
        title="1. Dataset Selection (Static Catalog)"
        collapsed={collapsed}
        onToggle={() => setCollapsed((current) => !current)}
        action={headerAction}
      />

      {!collapsed && (
        <>
          <form onSubmit={handleSubmit} className="panel-form">
            <select
              value={datasetId}
              onChange={(event) => {
                const nextValue = event.target.value;
                setDatasetId(nextValue);
                setError(null);
                if (!nextValue) {
                  onSelected(null);
                }
              }}
            >
              {datasetOptions.length === 0 ? (
                <option value="">No dataset configured</option>
              ) : (
                <>
                  <option value="">No dataset selected</option>
                  {datasetOptions.map((dataset) => (
                    <option key={dataset.dataset_id} value={dataset.dataset_id}>
                      {dataset.name}
                    </option>
                  ))}
                </>
              )}
            </select>
            <button type="submit" disabled={loading || datasetOptions.length === 0}>
              Load Dataset
            </button>
          </form>

          {error && <p className="error">{error}</p>}

          {datasetSummary && (
            <div className="summary-block">
              <p>
                <strong>Rows:</strong> {datasetSummary.row_count}
              </p>
              <CollapsibleSubsection
                title="Profile Sample"
                note={(
                  <p className="muted">
                    {rowCount > 0 ? `${sampleRatio.toFixed(2)}% of rows` : "-"}
                  </p>
                )}
                className="sample-config-card"
              >
                <div className="sample-config-grid">
                  <div>
                    <label htmlFor="sample-ratio">Sample ratio</label>
                    <input
                      id="sample-ratio"
                      type="range"
                      min="0.1"
                      max="5"
                      step="0.1"
                      value={rowCount > 0 ? sliderRatio : 0}
                      disabled={sampleLoading || !datasetSummary}
                      onChange={(event) => {
                        const nextRatio = Number.parseFloat(event.target.value);
                        const nextSampleSize = Math.max(
                          1,
                          Math.round((rowCount * nextRatio) / 100),
                        );
                        setSampleInput(String(nextSampleSize));
                        setSampleError(null);
                      }}
                    />
                  </div>
                  <div>
                    <label htmlFor="sample-size">Sample rows</label>
                    <input
                      id="sample-size"
                      type="number"
                      min={1}
                      max={Math.max(rowCount, 1)}
                      value={sampleInput}
                      disabled={sampleLoading || !datasetSummary}
                      onChange={(event) => {
                        setSampleInput(event.target.value);
                        setSampleError(null);
                      }}
                    />
                  </div>
                </div>
                <div className="sample-actions">
                  <button
                    type="button"
                    disabled={sampleLoading || !hasPendingSampleChange || requestedSampleSize === null}
                    onClick={() => {
                      void handleApplySample();
                    }}
                  >
                    Apply Sample
                  </button>
                  <button
                    type="button"
                    className="ghost-button"
                    disabled={sampleLoading || !hasPendingSampleChange}
                    onClick={handleResetSample}
                  >
                    Reset
                  </button>
                  {datasetSummary && (
                    <p className="muted">
                      Current sample: {datasetSummary.profile_sample_size.toLocaleString()} rows
                    </p>
                  )}
                </div>
                {sampleLoading && <p className="muted">Refreshing sampled statistics...</p>}
                {sampleError && <p className="error">{sampleError}</p>}
              </CollapsibleSubsection>
              <CollapsibleSubsection
                title="Columns"
                actions={(
                  <>
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={selectAllProfileColumns}
                      disabled={datasetColumns.length === 0}
                    >
                      Select all
                    </button>
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={clearProfileColumns}
                      disabled={selectedProfileColumns.length === 0}
                    >
                      Clear
                    </button>
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => setShowFullSchema((current) => !current)}
                    >
                      {showFullSchema ? "Hide full schema" : "Show full schema"}
                    </button>
                  </>
                )}
                note={(
                  <p className="muted">
                    {selectedProfileColumns.length}/{datasetSummary.columns.length} profiles selected
                  </p>
                )}
              >
                <div className="schema-pill-list">
                  {datasetSummary.columns.slice(0, 16).map((column) => (
                    <label key={column.name} className={`schema-pill ${selectedProfileSet.has(column.name) ? "is-selected" : ""}`}>
                      <input
                        type="checkbox"
                        checked={selectedProfileSet.has(column.name)}
                        onChange={() => toggleProfileColumn(column.name)}
                      />
                      <strong>{column.name}</strong>
                      <span>{column.inferred_type}</span>
                    </label>
                  ))}
                </div>
                {datasetSummary.columns.length > 16 && !showFullSchema && (
                  <p className="muted">
                    Showing first 16 columns. Open full schema to toggle the rest.
                  </p>
                )}
                {showFullSchema && (
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Select</th>
                          <th>Name</th>
                          <th>Type</th>
                          <th>Range</th>
                          <th>Distinct Ratio</th>
                        </tr>
                      </thead>
                      <tbody>
                        {datasetSummary.columns.map((column) => {
                          const profile = profileByName.get(column.name);
                          const nonNullCount = Math.max(
                            (profile?.sample_size ?? 0) - (profile?.null_count ?? 0),
                            0,
                          );
                          const distinctRatio = nonNullCount > 0
                            ? ((profile?.distinct_count ?? 0) / nonNullCount) * 100
                            : null;
                          const rangeLabel =
                            profile?.min_value && profile?.max_value
                              ? `${profile.min_value} - ${profile.max_value}`
                              : "-";

                          return (
                            <tr key={column.name}>
                              <td>
                                <input
                                  type="checkbox"
                                  checked={selectedProfileSet.has(column.name)}
                                  onChange={() => toggleProfileColumn(column.name)}
                                />
                              </td>
                              <td>{column.name}</td>
                              <td>{column.inferred_type}</td>
                              <td>{rangeLabel}</td>
                              <td>{distinctRatio === null ? "-" : formatDistinctRatio(distinctRatio)}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </CollapsibleSubsection>

              <DatasetProfilesPanel profiles={visibleProfiles} />
              <DatasetCorrelationPanel
                correlationSummary={correlationSummary}
                loading={correlationLoading}
                error={correlationError}
                onLoad={handleLoadCorrelation}
              />
            </div>
          )}
        </>
      )}
    </section>
  );
}
