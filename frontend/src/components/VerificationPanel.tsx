"use client";

import { ReactNode, useEffect, useMemo, useRef, useState } from "react";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { runMockLayoutExecution } from "../lib/api";
import {
  DatasetSummary,
  LayoutEvaluation,
  MockExecutionResult,
  WorkloadSummary,
} from "../lib/types";
import CollapsibleHeader from "./CollapsibleHeader";
import CollapsibleSubsection from "./CollapsibleSubsection";

type VerificationPanelProps = {
  datasetSummary: DatasetSummary | null;
  workloadSummary: WorkloadSummary | null;
  comparisonList: LayoutEvaluation[];
  onStatusChange?: (complete: boolean) => void;
  headerAction?: ReactNode;
};

const MOCK_PROGRESS_STEPS = ["Ingesting Data", "Running Queries", "Ranking"];
const SIMULATED_QUERY_COUNT = 1000;
const QUERIES_PER_SECOND = 10;
const ESTIMATED_RANK_COLOR = "#2f6f6f";
const REAL_RANK_COLOR = "#d17c45";
type MockProgressPhase = "idle" | "ingesting" | "queries" | "ranking" | "done";
type ExecutionLogEntry = {
  id: string;
  message: string;
};

type RuntimeSimulationPlan = {
  totalQueries: number;
  finalResults: MockExecutionResult[];
  cumulativeRuntimeMap: Map<string, number[]>;
};

type PreviewPlatformId = "hudi" | "delta" | "iceberg" | "spark";

type PreviewConfigTab = {
  id: PreviewPlatformId;
  label: string;
  content: string;
};

type SortDirection = "asc" | "desc";

type SelectedCandidateRow = {
  item: LayoutEvaluation;
  estimatedRank: number;
  realRank: number | null;
  partitionSpec: string;
  columnOrder: string;
  estimatedScore: number;
};

type SelectedCandidateSortKey =
  | "estimatedRank"
  | "realRank"
  | "partitionSpec"
  | "layoutType"
  | "columnOrder"
  | "estimatedScore"
  | "estimator";

type MockComparisonRow = {
  result: MockExecutionResult;
  estimatedRank: number;
  actualRank: number;
  rankDelta: number;
  estimatedScore: number;
  scoreErrorRatio: number;
  partitionSpec: string;
  columnOrder: string;
};

type MockComparisonSortKey =
  | "partitionSpec"
  | "layoutType"
  | "columnOrder"
  | "estimatedRank"
  | "actualRank"
  | "rankDelta"
  | "estimatedScore"
  | "actualRuntime"
  | "actualScore"
  | "scoreError"
  | "actualRecordRatio";

export default function VerificationPanel({
  datasetSummary,
  workloadSummary,
  comparisonList,
  onStatusChange,
  headerAction,
}: VerificationPanelProps) {
  const [collapsed, setCollapsed] = useState(false);
  const [selectedComparisonIds, setSelectedComparisonIds] = useState<string[]>([]);
  const [activeRunIds, setActiveRunIds] = useState<string[]>([]);
  const [mockRunStarted, setMockRunStarted] = useState(false);
  const [mockExecutionResults, setMockExecutionResults] = useState<MockExecutionResult[]>([]);
  const [displayedMockResults, setDisplayedMockResults] = useState<MockExecutionResult[]>([]);
  const [mockExecutionLoading, setMockExecutionLoading] = useState(false);
  const [mockExecutionError, setMockExecutionError] = useState<string | null>(null);
  const [mockProgressPhase, setMockProgressPhase] = useState<MockProgressPhase>("idle");
  const [executedQueryCount, setExecutedQueryCount] = useState(0);
  const [executionLogs, setExecutionLogs] = useState<ExecutionLogEntry[]>([]);
  const [selectedCandidatesSortKey, setSelectedCandidatesSortKey] =
    useState<SelectedCandidateSortKey>("estimatedRank");
  const [selectedCandidatesSortDirection, setSelectedCandidatesSortDirection] =
    useState<SortDirection>("asc");
  const [mockComparisonSortKey, setMockComparisonSortKey] =
    useState<MockComparisonSortKey>("actualRank");
  const [mockComparisonSortDirection, setMockComparisonSortDirection] =
    useState<SortDirection>("asc");
  const [previewConfigRow, setPreviewConfigRow] = useState<MockComparisonRow | null>(null);
  const [previewPlatformId, setPreviewPlatformId] = useState<PreviewPlatformId>("hudi");
  const [copiedPreviewConfig, setCopiedPreviewConfig] = useState(false);
  const progressTimerRef = useRef<number | null>(null);
  const mockProgressPhaseRef = useRef<MockProgressPhase>("idle");
  const executedQueryCountRef = useRef(0);
  const simulatedQueryCount = workloadSummary?.total_queries && workloadSummary.total_queries > 0
    ? workloadSummary.total_queries
    : SIMULATED_QUERY_COUNT;

  const pushExecutionLog = (message: string) => {
    const entry = {
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      message,
    };
    setExecutionLogs((current) => {
      const next = [...current, entry];
      return next.slice(-12);
    });
  };

  const clearProgressTimer = () => {
    if (progressTimerRef.current !== null) {
      window.clearInterval(progressTimerRef.current);
      progressTimerRef.current = null;
    }
  };

  const setProgressPhase = (phase: MockProgressPhase) => {
    mockProgressPhaseRef.current = phase;
    setMockProgressPhase(phase);
  };

  const sortedComparisonList = useMemo(
    () => [...comparisonList].sort((left, right) => getScoreValue(right) - getScoreValue(left)),
    [comparisonList],
  );

  const selectedComparisonItems = useMemo(
    () =>
      sortedComparisonList.filter((item) => selectedComparisonIds.includes(item.evaluation_id)),
    [selectedComparisonIds, sortedComparisonList],
  );
  const activeRunEvaluations = useMemo(
    () =>
      sortedComparisonList.filter((item) => activeRunIds.includes(item.evaluation_id)),
    [activeRunIds, sortedComparisonList],
  );

  useEffect(() => {
    setSelectedComparisonIds((current) =>
      current.filter((id) => comparisonList.some((item) => item.evaluation_id === id)),
    );
    setActiveRunIds((current) =>
      current.filter((id) => comparisonList.some((item) => item.evaluation_id === id)),
    );
  }, [comparisonList]);

  useEffect(() => {
    return () => {
      clearProgressTimer();
    };
  }, []);

  useEffect(() => {
    onStatusChange?.(mockExecutionResults.length > 0);
  }, [mockExecutionResults.length, onStatusChange]);

  const toggleComparisonSelection = (evaluationId: string) => {
    setSelectedComparisonIds((current) =>
      current.includes(evaluationId)
        ? current.filter((value) => value !== evaluationId)
        : [...current, evaluationId],
    );
  };

  const stopMockExecution = () => {
    clearProgressTimer();
    setMockExecutionLoading(false);
    setProgressPhase("idle");
    pushExecutionLog("Mock run stopped. You can restart with the same selected candidates.");
  };

  const openPreviewConfig = (row: MockComparisonRow) => {
    setPreviewConfigRow(row);
    setPreviewPlatformId("hudi");
    setCopiedPreviewConfig(false);
  };

  const closePreviewConfig = () => {
    setPreviewConfigRow(null);
    setCopiedPreviewConfig(false);
  };

  const copyPreviewConfig = async () => {
    if (!activePreviewTab) {
      return;
    }
    await navigator.clipboard.writeText(activePreviewTab.content);
    setCopiedPreviewConfig(true);
  };

  const handleRunMockExecution = async () => {
    if (!datasetSummary?.dataset_id) {
      setMockExecutionError("Load a dataset before running verification.");
      return;
    }
    if (!workloadSummary || workloadSummary.total_queries === 0) {
      setMockExecutionError("Load a workload before running verification.");
      return;
    }
    if (selectedComparisonItems.length === 0) {
      setMockExecutionError("Select at least one candidate from the comparison list.");
      return;
    }

    clearProgressTimer();
    setMockRunStarted(true);
    setActiveRunIds(selectedComparisonItems.map((item) => item.evaluation_id));
    setMockExecutionResults([]);
    setDisplayedMockResults([]);
    setMockExecutionLoading(true);
    setMockExecutionError(null);
    setExecutedQueryCount(0);
    executedQueryCountRef.current = 0;
    setExecutionLogs([]);
    setProgressPhase("ingesting");
    pushExecutionLog(`Starting mock runner for ${selectedComparisonItems.length} selected candidates.`);
    pushExecutionLog(`Ingesting data for dataset ${datasetSummary.dataset_id}.`);

    try {
      const response = await runMockLayoutExecution({
        dataset_id: datasetSummary.dataset_id,
        candidates: selectedComparisonItems.map((item) => ({
          evaluation_id: item.evaluation_id,
          partition_strategy: item.partition_strategy,
          partition_columns: item.partition_columns,
          layout_type: item.layout_type,
          layout_columns: item.layout_columns,
          estimated_score: getScoreValue(item),
          avg_record_read_ratio: item.avg_record_read_ratio,
          avg_row_group_read_ratio: item.avg_row_group_read_ratio,
          layout_complexity: item.layout_complexity,
        })),
      });
      const orderedResults = [...response.results].sort(
        (left, right) => left.actual_runtime_ms - right.actual_runtime_ms,
      );
      setMockExecutionResults(orderedResults);
      const simulationPlan = buildRuntimeSimulationPlan(
        orderedResults,
        simulatedQueryCount,
      );
      setDisplayedMockResults(buildSimulationSnapshot(simulationPlan, 0));
      pushExecutionLog(`Mock runner ready. Simulating ${simulatedQueryCount} workload queries at ${QUERIES_PER_SECOND} queries/sec.`);

      let ingestTicks = 0;
      progressTimerRef.current = window.setInterval(() => {
        const currentPhase = mockProgressPhaseRef.current;
        if (currentPhase === "ingesting") {
          ingestTicks += 1;
          if (ingestTicks >= 2) {
            setProgressPhase("queries");
            pushExecutionLog(`Running query 1/${simulatedQueryCount} across ${selectedComparisonItems.length} candidates.`);
          }
          return;
        }

        if (currentPhase === "queries") {
          const nextCount = Math.min(
            simulatedQueryCount,
            executedQueryCountRef.current + QUERIES_PER_SECOND,
          );
          if (nextCount !== executedQueryCountRef.current) {
            executedQueryCountRef.current = nextCount;
            setExecutedQueryCount(nextCount);
            setDisplayedMockResults(buildSimulationSnapshot(simulationPlan, nextCount));
            pushExecutionLog(`Executed query ${nextCount}/${simulatedQueryCount}.`);
          }
          if (nextCount >= simulatedQueryCount) {
            setProgressPhase("ranking");
            setDisplayedMockResults(orderedResults);
            pushExecutionLog(`All ${simulatedQueryCount} simulated queries finished. Final ranking locked.`);
            pushExecutionLog(`Fastest actual candidate: ${formatMockResultLabel(orderedResults[0])}.`);
          }
          return;
        }

        if (currentPhase === "ranking") {
          clearProgressTimer();
          setProgressPhase("done");
          setMockExecutionLoading(false);
        }
      }, 1000);
    } catch (err) {
      setMockExecutionError(
        err instanceof Error ? err.message : "Verification failed.",
      );
      setMockExecutionResults([]);
      setDisplayedMockResults([]);
      setActiveRunIds(selectedComparisonItems.map((item) => item.evaluation_id));
      setProgressPhase("idle");
      setMockExecutionLoading(false);
      pushExecutionLog("Mock execution failed before ranking completed.");
    }
  };

  const actualRankMap = useMemo(
    () => buildActualRankMap(displayedMockResults),
    [displayedMockResults],
  );
  const selectedCandidateRows = useMemo(
    () =>
      sortedComparisonList.map((item, index) => ({
        item,
        estimatedRank: index + 1,
        realRank: actualRankMap.get(item.evaluation_id) ?? null,
        partitionSpec: formatPartitionSpec(item.partition_strategy, item.partition_columns),
        columnOrder: item.layout_columns.length > 0 ? item.layout_columns.join(" -> ") : "-",
        estimatedScore: getScoreValue(item),
      })),
    [actualRankMap, sortedComparisonList],
  );
  const sortedSelectedCandidateRows = useMemo(
    () =>
      sortSelectedCandidateRows(
        selectedCandidateRows,
        selectedCandidatesSortKey,
        selectedCandidatesSortDirection,
      ),
    [selectedCandidateRows, selectedCandidatesSortDirection, selectedCandidatesSortKey],
  );
  const comparisonRows = useMemo(
    () => buildMockComparisonRows(activeRunEvaluations, displayedMockResults),
    [activeRunEvaluations, displayedMockResults],
  );
  const sortedComparisonRows = useMemo(
    () =>
      sortMockComparisonRows(
        comparisonRows,
        mockComparisonSortKey,
        mockComparisonSortDirection,
      ),
    [comparisonRows, mockComparisonSortDirection, mockComparisonSortKey],
  );
  const rankChartData = useMemo(
    () => buildRankChartData(activeRunEvaluations, actualRankMap),
    [activeRunEvaluations, actualRankMap],
  );
  const previewTabs = useMemo(
    () => previewConfigRow ? buildPreviewConfigTabs(previewConfigRow) : [],
    [previewConfigRow],
  );
  const activePreviewTab = useMemo(
    () => previewTabs.find((tab) => tab.id === previewPlatformId) ?? previewTabs[0] ?? null,
    [previewPlatformId, previewTabs],
  );
  const rankOverviewVisible = mockRunStarted && activeRunEvaluations.length > 0;
  const progressPercent = useMemo(() => {
    if (!rankOverviewVisible) {
      return 0;
    }
    if (mockProgressPhase === "ingesting") {
      return 10;
    }
    if (mockProgressPhase === "queries") {
      return 10 + ((executedQueryCount / Math.max(simulatedQueryCount, 1)) * 72);
    }
    if (mockProgressPhase === "ranking") {
      return 96;
    }
    const totalResults = Math.max(mockExecutionResults.length, activeRunEvaluations.length, 1);
    const revealedRatio = displayedMockResults.length / totalResults;
    return 84 + revealedRatio * 16;
  }, [
    activeRunEvaluations.length,
    displayedMockResults.length,
    executedQueryCount,
    mockExecutionLoading,
    mockExecutionResults.length,
    mockProgressPhase,
    simulatedQueryCount,
    rankOverviewVisible,
  ]);
  const selectedCandidatesSortArrow = selectedCandidatesSortDirection === "asc" ? "↑" : "↓";
  const mockComparisonSortArrow = mockComparisonSortDirection === "asc" ? "↑" : "↓";

  const toggleSelectedCandidatesSort = (nextKey: SelectedCandidateSortKey) => {
    if (selectedCandidatesSortKey === nextKey) {
      setSelectedCandidatesSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setSelectedCandidatesSortKey(nextKey);
    setSelectedCandidatesSortDirection(
      nextKey === "partitionSpec" || nextKey === "layoutType" || nextKey === "columnOrder" || nextKey === "estimator"
        ? "asc"
        : nextKey === "estimatedScore"
          ? "desc"
          : "asc",
    );
  };

  const toggleMockComparisonSort = (nextKey: MockComparisonSortKey) => {
    if (mockComparisonSortKey === nextKey) {
      setMockComparisonSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setMockComparisonSortKey(nextKey);
    setMockComparisonSortDirection(
      nextKey === "partitionSpec" || nextKey === "layoutType" || nextKey === "columnOrder"
        ? "asc"
        : nextKey === "estimatedScore" || nextKey === "actualScore"
          ? "desc"
          : "asc",
    );
  };

  return (
    <section className="panel">
      <CollapsibleHeader
        title="4. Verification"
        collapsed={collapsed}
        onToggle={() => setCollapsed((current) => !current)}
        action={headerAction}
      />

      {!collapsed && (
        <>
          <CollapsibleSubsection title="Selected Candidates">
            {sortedComparisonList.length > 0 && (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Select</th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleSelectedCandidatesSort("estimatedRank")}>
                          Estimated Rank {selectedCandidatesSortKey === "estimatedRank" ? selectedCandidatesSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleSelectedCandidatesSort("realRank")}>
                          Real Rank {selectedCandidatesSortKey === "realRank" ? selectedCandidatesSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleSelectedCandidatesSort("partitionSpec")}>
                          Partition Spec {selectedCandidatesSortKey === "partitionSpec" ? selectedCandidatesSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleSelectedCandidatesSort("layoutType")}>
                          Layout Type {selectedCandidatesSortKey === "layoutType" ? selectedCandidatesSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleSelectedCandidatesSort("columnOrder")}>
                          Column Order {selectedCandidatesSortKey === "columnOrder" ? selectedCandidatesSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleSelectedCandidatesSort("estimatedScore")}>
                          Estimated Score {selectedCandidatesSortKey === "estimatedScore" ? selectedCandidatesSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleSelectedCandidatesSort("estimator")}>
                          Estimator {selectedCandidatesSortKey === "estimator" ? selectedCandidatesSortArrow : ""}
                        </button>
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedSelectedCandidateRows.map((row) => (
                      <tr key={row.item.evaluation_id}>
                        <td>
                          <input
                            type="checkbox"
                            checked={selectedComparisonIds.includes(row.item.evaluation_id)}
                            onChange={() => toggleComparisonSelection(row.item.evaluation_id)}
                          />
                        </td>
                        <td>{row.estimatedRank}</td>
                        <td>{row.realRank ?? "-"}</td>
                        <td>{row.partitionSpec}</td>
                        <td>{row.item.layout_type}</td>
                        <td>{row.columnOrder}</td>
                        <td>{row.estimatedScore.toFixed(3)}</td>
                        <td>{row.item.algorithm}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <div className="sample-actions">
              <button
                type="button"
                disabled={sortedComparisonList.length === 0 || selectedComparisonIds.length === 0 || mockExecutionLoading}
                onClick={() => {
                  void handleRunMockExecution();
                }}
              >
                Run Mock Actual Benchmark
              </button>
              <button
                type="button"
                className="ghost-button"
                disabled={!mockExecutionLoading}
                onClick={stopMockExecution}
              >
                Stop Mock Run
              </button>
            </div>
            {mockExecutionError && <p className="error">{mockExecutionError}</p>}
          </CollapsibleSubsection>

          {rankOverviewVisible && (
            <CollapsibleSubsection title="Rank Overview" className="chart-card chart-card-full">
              <div className="verification-progress">
                <div className="verification-progress-head">
                  <strong>{getProgressHeadline(mockProgressPhase, executedQueryCount, simulatedQueryCount)}</strong>
                  <span className="muted">
                    {mockExecutionLoading
                      ? "mock benchmark in progress"
                      : `${displayedMockResults.length}/${Math.max(mockExecutionResults.length, activeRunEvaluations.length)} candidates ranked`}
                  </span>
                </div>
                <div className="verification-progress-bar" aria-hidden="true">
                  <span style={{ width: `${Math.min(progressPercent, 100)}%` }} />
                </div>
                <div className="verification-progress-steps">
                  {MOCK_PROGRESS_STEPS.map((label, index) => {
                    const currentStepIndex = getProgressStepIndex(mockProgressPhase);
                    const stateClass = index < currentStepIndex
                      ? "is-complete"
                      : index === currentStepIndex
                        ? "is-active"
                        : "";
                    return (
                      <span key={label} className={`verification-progress-step ${stateClass}`}>
                        {label}
                      </span>
                    );
                  })}
                </div>
                <div className="verification-log-panel">
                  {executionLogs.length === 0 ? (
                    <p className="muted">Execution logs will appear after you start the mock benchmark.</p>
                  ) : (
                    executionLogs.map((entry) => (
                      <p key={entry.id} className="verification-log-line">
                        {entry.message}
                      </p>
                    ))
                  )}
                </div>
              </div>

              <div
                className="chart-wrap chart-wrap-tall"
                style={{ height: Math.max(300, rankChartData.length * 34) }}
              >
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={rankChartData}
                    layout="vertical"
                    margin={{ top: 8, right: 24, bottom: 8, left: 52 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis type="number" allowDecimals={false} />
                    <YAxis
                      type="category"
                      dataKey="name"
                      width={220}
                      tick={{ fontSize: 11 }}
                    />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="estimatedRank" name="Estimated Rank" fill={ESTIMATED_RANK_COLOR} />
                    <Bar dataKey="realRank" name="Real Rank" fill={REAL_RANK_COLOR} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CollapsibleSubsection>
          )}

          {comparisonRows.length > 0 && (
            <CollapsibleSubsection title="Estimated vs Mock Actual">
              <div className="table-wrap">
                <table className="verification-results-table">
                  <thead>
                    <tr>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("partitionSpec")}>
                          Partition Spec {mockComparisonSortKey === "partitionSpec" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("layoutType")}>
                          Layout Type {mockComparisonSortKey === "layoutType" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("columnOrder")}>
                          Column Order {mockComparisonSortKey === "columnOrder" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("estimatedRank")}>
                          Estimated Rank {mockComparisonSortKey === "estimatedRank" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("actualRank")}>
                          Actual Rank {mockComparisonSortKey === "actualRank" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("rankDelta")}>
                          Rank Delta {mockComparisonSortKey === "rankDelta" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("estimatedScore")}>
                          Estimated Score {mockComparisonSortKey === "estimatedScore" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("actualRuntime")}>
                          Actual Runtime {mockComparisonSortKey === "actualRuntime" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("actualScore")}>
                          Actual Score {mockComparisonSortKey === "actualScore" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("scoreError")}>
                          Score Error {mockComparisonSortKey === "scoreError" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>
                        <button type="button" className="table-sort-button" onClick={() => toggleMockComparisonSort("actualRecordRatio")}>
                          Actual Record Ratio {mockComparisonSortKey === "actualRecordRatio" ? mockComparisonSortArrow : ""}
                        </button>
                      </th>
                      <th>Preview Config</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedComparisonRows.map((row) => (
                      <tr key={`verification-${row.result.evaluation_id}`}>
                        <td>{row.partitionSpec}</td>
                        <td>{row.result.layout_type}</td>
                        <td>{row.columnOrder}</td>
                        <td>{row.estimatedRank}</td>
                        <td>{row.actualRank}</td>
                        <td>{row.rankDelta > 0 ? `+${row.rankDelta}` : row.rankDelta}</td>
                        <td>{row.estimatedScore.toFixed(3)}</td>
                        <td>{row.result.actual_runtime_ms.toFixed(1)} ms</td>
                        <td>{row.result.actual_score.toFixed(3)}</td>
                        <td>{(row.scoreErrorRatio * 100).toFixed(1)}%</td>
                        <td>{(row.result.actual_records_read_ratio * 100).toFixed(1)}%</td>
                        <td>
                          <button
                            type="button"
                            className="ghost-button table-action-button"
                            onClick={() => openPreviewConfig(row)}
                          >
                            Preview
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CollapsibleSubsection>
          )}

          {previewConfigRow && activePreviewTab && (
            <div
              className="preview-config-modal-backdrop"
              role="presentation"
              onClick={closePreviewConfig}
            >
              <div
                className="preview-config-modal"
                role="dialog"
                aria-modal="true"
                aria-labelledby="preview-config-title"
                onClick={(event) => event.stopPropagation()}
              >
                <div className="preview-config-modal-head">
                  <div>
                    <h3 id="preview-config-title">Preview Config</h3>
                    <p className="muted">
                      {previewConfigRow.result.layout_type} | {previewConfigRow.columnOrder}
                    </p>
                  </div>
                  <button type="button" className="ghost-button" onClick={closePreviewConfig}>
                    Close
                  </button>
                </div>

                <div className="preview-config-tabs">
                  {previewTabs.map((tab) => (
                    <button
                      key={tab.id}
                      type="button"
                      className={`preview-config-tab ${previewPlatformId === tab.id ? "is-active" : ""}`}
                      onClick={() => {
                        setPreviewPlatformId(tab.id);
                        setCopiedPreviewConfig(false);
                      }}
                    >
                      {tab.label}
                    </button>
                  ))}
                </div>

                <div className="preview-config-meta">
                  <span><strong>Partition:</strong> {previewConfigRow.partitionSpec}</span>
                  <span><strong>Layout:</strong> {previewConfigRow.result.layout_type}</span>
                  <span><strong>Order:</strong> {previewConfigRow.columnOrder}</span>
                </div>

                <div className="preview-config-actions">
                  <button type="button" onClick={() => { void copyPreviewConfig(); }}>
                    {copiedPreviewConfig ? "Copied" : "Copy Config"}
                  </button>
                </div>

                <pre className="preview-config-code">
                  <code>{activePreviewTab.content}</code>
                </pre>
              </div>
            </div>
          )}
        </>
      )}
    </section>
  );
}

function getScoreValue(item: LayoutEvaluation): number {
  return item.composite_score ?? (1 - item.avg_record_read_ratio);
}

function buildMockComparisonRows(
  evaluations: LayoutEvaluation[],
  results: MockExecutionResult[],
): MockComparisonRow[] {
  const estimatedRankMap = new Map(
    evaluations.map((item, index) => [item.evaluation_id, index + 1]),
  );
  const actualRankMap = buildActualRankMap(results);
  const evaluationMap = new Map(evaluations.map((item) => [item.evaluation_id, item]));

  return [...results]
    .sort((left, right) => left.actual_runtime_ms - right.actual_runtime_ms)
    .map((result) => {
      const evaluation = evaluationMap.get(result.evaluation_id);
      const estimatedScore = evaluation ? getScoreValue(evaluation) : result.actual_score;
      return {
        result,
        estimatedRank: estimatedRankMap.get(result.evaluation_id) ?? 0,
        actualRank: actualRankMap.get(result.evaluation_id) ?? 0,
        rankDelta:
          (estimatedRankMap.get(result.evaluation_id) ?? 0)
          - (actualRankMap.get(result.evaluation_id) ?? 0),
        estimatedScore,
        scoreErrorRatio:
          estimatedScore > 0
            ? Math.abs(result.actual_score - estimatedScore) / estimatedScore
            : 0,
        partitionSpec: formatPartitionSpec(result.partition_strategy, result.partition_columns),
        columnOrder: result.layout_columns.length > 0 ? result.layout_columns.join(" -> ") : "-",
      };
    });
}

function buildActualRankMap(results: MockExecutionResult[]): Map<string, number> {
  return new Map(
    [...results]
      .sort((left, right) => left.actual_runtime_ms - right.actual_runtime_ms)
      .map((item, index) => [item.evaluation_id, index + 1]),
  );
}

function buildRankChartData(
  evaluations: LayoutEvaluation[],
  actualRankMap: Map<string, number>,
): Array<{ name: string; estimatedRank: number; realRank: number | null }> {
  return evaluations
    .map((item, index) => ({
      name: formatCandidateLabel(item),
      estimatedRank: index + 1,
      realRank: actualRankMap.get(item.evaluation_id) ?? null,
    }))
    .sort((left, right) => {
      const leftRank = left.realRank ?? Number.POSITIVE_INFINITY;
      const rightRank = right.realRank ?? Number.POSITIVE_INFINITY;
      return leftRank - rightRank || left.estimatedRank - right.estimatedRank;
    });
}

function formatPartitionSpec(partitionStrategy: string, partitionColumns: string[]): string {
  if (partitionStrategy === "none" || partitionColumns.length === 0) {
    return "none";
  }
  return `${partitionStrategy}(${partitionColumns.join(", ")})`;
}

function formatCandidateLabel(item: LayoutEvaluation): string {
  const layoutColumns = item.layout_columns.length > 0 ? item.layout_columns.join(" -> ") : "no layout";
  return `${item.layout_type} | ${layoutColumns}`;
}

function formatMockResultLabel(result: MockExecutionResult): string {
  const layoutColumns = result.layout_columns.length > 0
    ? result.layout_columns.join(" -> ")
    : "no layout";
  return `${result.layout_type} | ${layoutColumns}`;
}

function buildPreviewConfigTabs(row: MockComparisonRow): PreviewConfigTab[] {
  const tabs: PreviewConfigTab[] = [
    { id: "hudi", label: "Hudi", content: buildHudiPreviewConfig(row) },
    { id: "delta", label: "Delta Lake", content: buildDeltaLakePreviewConfig(row) },
    { id: "spark", label: "Spark", content: buildSparkPreviewConfig(row) },
  ];

  if (row.result.layout_type !== "hilbert") {
    tabs.splice(2, 0, {
      id: "iceberg",
      label: "Iceberg",
      content: buildIcebergPreviewConfig(row),
    });
  }

  return tabs;
}

function buildHudiPreviewConfig(row: MockComparisonRow): string {
  const partitionColumns = toCsvList(row.result.partition_columns);
  const layoutColumns = toCsvList(row.result.layout_columns);
  const layoutType = row.result.layout_type;

  if (layoutType === "no layout") {
    return [
      "# Hudi baseline write preview",
      `hoodie.datasource.write.partitionpath.field = ${partitionColumns || "<none>"}`,
      `hoodie.datasource.write.hive_style_partitioning = ${partitionColumns ? "true" : "false"}`,
      "hoodie.clustering.inline = false",
      "hoodie.clustering.layout.optimize.enable = false",
      "# No in-file layout optimization selected",
    ].join("\n");
  }

  return [
    "# Hudi clustering preview",
    `hoodie.datasource.write.partitionpath.field = ${partitionColumns || "<none>"}`,
    `hoodie.datasource.write.hive_style_partitioning = ${partitionColumns ? "true" : "false"}`,
    "hoodie.clustering.inline = true",
    "hoodie.clustering.schedule.inline = true",
    "hoodie.clustering.layout.optimize.enable = true",
    `hoodie.clustering.layout.optimize.strategy = ${layoutType}`,
    `hoodie.clustering.plan.strategy.sort.columns = ${layoutColumns || "<none>"}`,
    "hoodie.clustering.execution.strategy.class = org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
  ].join("\n");
}

function buildDeltaLakePreviewConfig(row: MockComparisonRow): string {
  const partitionColumns = toSqlList(row.result.partition_columns);
  const layoutColumns = toSqlList(row.result.layout_columns);
  const layoutType = row.result.layout_type;

  return [
    "-- Delta Lake preview",
    partitionColumns
      ? `CREATE OR REPLACE TABLE <catalog>.<schema>.<table_name>\nUSING DELTA\nPARTITIONED BY (${partitionColumns})\nAS SELECT * FROM <source_table>;`
      : `CREATE OR REPLACE TABLE <catalog>.<schema>.<table_name>\nUSING DELTA\nAS SELECT * FROM <source_table>;`,
    "",
    "ALTER TABLE <catalog>.<schema>.<table_name> SET TBLPROPERTIES (",
    "  'delta.autoOptimize.optimizeWrite' = 'true',",
    "  'delta.autoOptimize.autoCompact' = 'true'",
    ");",
    layoutType === "zorder"
      ? `OPTIMIZE <catalog>.<schema>.<table_name>\nZORDER BY (${layoutColumns || "<layout_columns>"});`
      : layoutType === "no layout"
        ? "-- No in-file layout optimization selected"
        : `-- Native ${layoutType} knobs are not exposed directly here; fallback to clustering by the chosen order\nALTER TABLE <catalog>.<schema>.<table_name>\nCLUSTER BY (${layoutColumns || "<layout_columns>"});`,
  ].join("\n");
}

function buildIcebergPreviewConfig(row: MockComparisonRow): string {
  const partitionColumns = toSqlList(row.result.partition_columns);
  const layoutColumns = toSqlList(row.result.layout_columns);
  const layoutType = row.result.layout_type;

  return [
    "-- Iceberg write-layout preview",
    partitionColumns
      ? `CREATE TABLE <catalog>.<schema>.<table_name>\nUSING iceberg\nPARTITIONED BY (${partitionColumns})\nAS SELECT * FROM <source_table>;`
      : `CREATE TABLE <catalog>.<schema>.<table_name>\nUSING iceberg\nAS SELECT * FROM <source_table>;`,
    "",
    "ALTER TABLE <catalog>.<schema>.<table_name> SET TBLPROPERTIES (",
    `  'write.distribution-mode' = '${partitionColumns ? "hash" : "none"}',`,
    "  'write.spark.fanout.enabled' = 'true'",
    ");",
    layoutType === "no layout"
      ? "-- No ordering clause selected"
      : `ALTER TABLE <catalog>.<schema>.<table_name>\nWRITE ORDERED BY (${layoutColumns || "<layout_columns>"});`,
    layoutType === "zorder"
      ? "-- Z-order is approximated here with ordered writes on the chosen columns"
      : `-- Requested layout strategy: ${layoutType}`,
  ].join("\n");
}

function buildSparkPreviewConfig(row: MockComparisonRow): string {
  const partitionColumns = toQuotedCsvList(row.result.partition_columns);
  const layoutColumns = toQuotedCsvList(row.result.layout_columns);
  const layoutType = row.result.layout_type;

  return [
    "# Spark DataFrame writer preview",
    "writer = (",
    "  source_df",
    partitionColumns ? `    .repartition(${partitionColumns})` : "    # no partition repartition selected",
    layoutType === "no layout"
      ? "    # no explicit in-file layout ordering selected"
      : `    .sortWithinPartitions(${layoutColumns || "\"<layout_columns>\""})`,
    "    .write",
    "    .format(\"parquet\")",
    partitionColumns ? `    .partitionBy(${partitionColumns})` : "    # no partitionBy clause",
    "    .mode(\"overwrite\")",
    "    .save(\"<target_path>\")",
    ")",
    "",
    `# intended layout strategy: ${layoutType}`,
  ].join("\n");
}

function toCsvList(values: string[]): string {
  return values.join(",");
}

function toSqlList(values: string[]): string {
  return values.join(", ");
}

function toQuotedCsvList(values: string[]): string {
  return values.map((value) => `"${value}"`).join(", ");
}

function buildRuntimeSimulationPlan(
  results: MockExecutionResult[],
  totalQueries: number,
): RuntimeSimulationPlan {
  const cumulativeRuntimeMap = new Map<string, number[]>();

  results.forEach((result) => {
    const hash = simpleHash(result.evaluation_id);
    const weights = Array.from({ length: totalQueries }, (_, index) => {
      const sinPart = Math.sin((index + 1) * ((hash % 7) + 1) * 0.071);
      const cosPart = Math.cos((index + 1) * ((hash % 11) + 3) * 0.053);
      return Math.max(0.18, 1 + sinPart * 0.28 + cosPart * 0.16);
    });
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);
    const scale = totalWeight > 0 ? result.actual_runtime_ms / totalWeight : 0;

    let cumulative = 0;
    const cumulativeRuntimes = weights.map((weight) => {
      cumulative += weight * scale;
      return cumulative;
    });
    cumulativeRuntimeMap.set(result.evaluation_id, cumulativeRuntimes);
  });

  return {
    totalQueries,
    finalResults: results,
    cumulativeRuntimeMap,
  };
}

function buildSimulationSnapshot(
  plan: RuntimeSimulationPlan,
  executedQueries: number,
): MockExecutionResult[] {
  const safeQueryIndex = Math.max(0, Math.min(executedQueries, plan.totalQueries));
  const progressRatio = plan.totalQueries > 0 ? safeQueryIndex / plan.totalQueries : 0;

  return plan.finalResults.map((result) => {
    const runtimeSeries = plan.cumulativeRuntimeMap.get(result.evaluation_id) ?? [];
    const simulatedRuntime = safeQueryIndex === 0
      ? 0
      : (runtimeSeries[safeQueryIndex - 1] ?? result.actual_runtime_ms);

    return {
      ...result,
      actual_runtime_ms: simulatedRuntime,
      actual_score: result.actual_score * progressRatio,
      actual_records_read_ratio: result.actual_records_read_ratio * progressRatio,
      actual_row_group_read_ratio: result.actual_row_group_read_ratio * progressRatio,
    };
  });
}

function getProgressStepIndex(phase: MockProgressPhase): number {
  switch (phase) {
    case "ingesting":
      return 0;
    case "queries":
      return 1;
    case "ranking":
    case "done":
      return 2;
    default:
      return -1;
  }
}

function getProgressHeadline(
  phase: MockProgressPhase,
  executedQueryCount: number,
  simulatedQueryCount: number,
): string {
  switch (phase) {
    case "ingesting":
      return "Ingesting data";
    case "queries":
      return `Executing query ${Math.min(executedQueryCount, simulatedQueryCount)} / ${simulatedQueryCount}`;
    case "ranking":
      return "Ranking candidates";
    case "done":
      return "Ranking completed";
    default:
      return "Ready to run";
  }
}

function sortSelectedCandidateRows(
  rows: SelectedCandidateRow[],
  sortKey: SelectedCandidateSortKey,
  direction: SortDirection,
): SelectedCandidateRow[] {
  const multiplier = direction === "asc" ? 1 : -1;
  return [...rows].sort((left, right) => {
    let result = 0;
    switch (sortKey) {
      case "estimatedRank":
        result = left.estimatedRank - right.estimatedRank;
        break;
      case "realRank":
        result = compareNullableNumber(left.realRank, right.realRank);
        break;
      case "partitionSpec":
        result = left.partitionSpec.localeCompare(right.partitionSpec);
        break;
      case "layoutType":
        result = left.item.layout_type.localeCompare(right.item.layout_type);
        break;
      case "columnOrder":
        result = left.columnOrder.localeCompare(right.columnOrder);
        break;
      case "estimatedScore":
        result = left.estimatedScore - right.estimatedScore;
        break;
      case "estimator":
        result = left.item.algorithm.localeCompare(right.item.algorithm);
        break;
      default:
        result = 0;
    }

    if (result === 0) {
      result = left.item.evaluation_id.localeCompare(right.item.evaluation_id);
    }
    return result * multiplier;
  });
}

function sortMockComparisonRows(
  rows: MockComparisonRow[],
  sortKey: MockComparisonSortKey,
  direction: SortDirection,
): MockComparisonRow[] {
  const multiplier = direction === "asc" ? 1 : -1;
  return [...rows].sort((left, right) => {
    let result = 0;
    switch (sortKey) {
      case "partitionSpec":
        result = left.partitionSpec.localeCompare(right.partitionSpec);
        break;
      case "layoutType":
        result = left.result.layout_type.localeCompare(right.result.layout_type);
        break;
      case "columnOrder":
        result = left.columnOrder.localeCompare(right.columnOrder);
        break;
      case "estimatedRank":
        result = left.estimatedRank - right.estimatedRank;
        break;
      case "actualRank":
        result = left.actualRank - right.actualRank;
        break;
      case "rankDelta":
        result = left.rankDelta - right.rankDelta;
        break;
      case "estimatedScore":
        result = left.estimatedScore - right.estimatedScore;
        break;
      case "actualRuntime":
        result = left.result.actual_runtime_ms - right.result.actual_runtime_ms;
        break;
      case "actualScore":
        result = left.result.actual_score - right.result.actual_score;
        break;
      case "scoreError":
        result = left.scoreErrorRatio - right.scoreErrorRatio;
        break;
      case "actualRecordRatio":
        result = left.result.actual_records_read_ratio - right.result.actual_records_read_ratio;
        break;
      default:
        result = 0;
    }

    if (result === 0) {
      result = left.result.evaluation_id.localeCompare(right.result.evaluation_id);
    }
    return result * multiplier;
  });
}

function compareNullableNumber(left: number | null, right: number | null): number {
  const safeLeft = left ?? Number.POSITIVE_INFINITY;
  const safeRight = right ?? Number.POSITIVE_INFINITY;
  return safeLeft - safeRight;
}

function simpleHash(input: string): number {
  let hash = 0;
  for (let index = 0; index < input.length; index += 1) {
    hash = ((hash << 5) - hash + input.charCodeAt(index)) | 0;
  }
  return Math.abs(hash);
}
