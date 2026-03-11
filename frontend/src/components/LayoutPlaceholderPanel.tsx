"use client";

import { CSSProperties, ReactNode, useEffect, useMemo, useState } from "react";

import { evaluateLayout } from "../lib/api";
import {
  CorrelationSummary,
  DatasetSummary,
  LayoutEvaluation,
  LayoutEvaluationResponse,
  WorkloadSummary,
} from "../lib/types";
import CollapsibleHeader from "./CollapsibleHeader";
import CollapsibleSubsection from "./CollapsibleSubsection";

type LayoutPlaceholderPanelProps = {
  columns: string[];
  datasetSummary: DatasetSummary | null;
  workloadSummary: WorkloadSummary | null;
  onComparisonListChange?: (items: LayoutEvaluation[]) => void;
  onGlobalLoadingStart?: (label: string) => void;
  onGlobalLoadingEnd?: () => void;
};

type LayoutPlatform = {
  name: string;
  short: string;
  tone: string;
};

const partitionStrategyOptions = [
  {
    id: "none",
    label: "No Partition",
    description: "Keep the dataset unpartitioned and rely only on layout inside files.",
  },
  {
    id: "value",
    label: "Value Partition",
    description: "Split data by the chosen partition columns first, then evaluate layout within touched partitions.",
  },
] as const;

const layoutOptions = [
  {
    id: "no layout",
    label: "No Layout",
    description: "Unordered baseline without a learned space-filling path.",
    platforms: [] as LayoutPlatform[],
  },
  {
    id: "zorder",
    label: "Z-Order",
    description: "Morton-style quadrant traversal that preserves locality coarsely.",
    platforms: [
      { name: "Databricks", short: "DB", tone: "databricks" },
      { name: "Hudi", short: "HU", tone: "hudi" },
      { name: "Iceberg", short: "IC", tone: "iceberg" },
      { name: "AWS", short: "AWS", tone: "aws" },
    ],
  },
  {
    id: "linear",
    label: "Linear",
    description: "Simple lexicographic ordering across the selected columns.",
    platforms: [
      { name: "Hudi", short: "HU", tone: "hudi" },
      { name: "Iceberg", short: "IC", tone: "iceberg" },
      { name: "Snowflake", short: "SN", tone: "snowflake" },
      { name: "AWS", short: "AWS", tone: "aws" },
    ],
  },
  {
    id: "hilbert",
    label: "Hilbert",
    description: "Continuous space-filling curve with stronger locality preservation.",
    platforms: [
      { name: "Hudi", short: "HU", tone: "hudi" },
      { name: "DuckDB", short: "DU", tone: "duckdb" },
    ],
  },
] as const;

const MAX_LAYOUT_PERMUTATIONS = 120;
const MAX_ENUMERATED_LAYOUT_PERMUTATIONS = 4000;

type LayoutCandidate = {
  key: string;
  columns: string[];
  estimatedUtilityScore: number;
  correlationCohesion: number;
};

type ScoreSortDirection = "asc" | "desc";
type LayoutSortKey =
  | "name"
  | "type"
  | "distinctRatio"
  | "rangeShare"
  | "avgPredicateSelectivity"
  | "avgQuerySelectivity"
  | "correlationStrength"
  | "orderingUtility";

type DesignColumnRow = {
  name: string;
  inferredType: string;
  distinctRatio: number;
  workloadFrequency: number;
  workloadFrequencyPercent: number;
  equalityCount: number;
  inListCount: number;
  rangeCount: number;
  equalityInShare: number;
  rangeShare: number;
  avgPredicateSelectivity: number | null;
  avgQuerySelectivity: number | null;
  partitionRisk: PartitionRisk;
  partitionHint: UtilityHint;
  orderingUtility: UtilityHint;
  correlationStrength: number;
  correlationGroupSize: number;
  correlationColor: string | null;
};

type UtilityHint = {
  level: "strong" | "conditional" | "weak";
  label: string;
  reason: string;
  score: number;
};

type PartitionRisk = {
  level: "low" | "medium" | "high";
  label: string;
  reason: string;
  score: number;
};

const DISTINCT_RATIO_DECIMALS = 3;

type DesignStageId = "partition" | "layout";

type DesignTimelineStageProps = {
  title: string;
  subtitle: string;
  description: ReactNode;
  summary: ReactNode;
  collapsed: boolean;
  onToggle: () => void;
  active: boolean;
  showSegment: boolean;
  children: ReactNode;
};

function DesignTimelineStage({
  title,
  subtitle,
  description,
  summary,
  collapsed,
  onToggle,
  active,
  showSegment,
  children,
}: DesignTimelineStageProps) {
  return (
    <article className={`design-timeline-stage ${active ? "is-active" : ""}`}>
      <button type="button" className="design-stage-toggle" onClick={onToggle}>
        <div className="design-streamline-rail" aria-hidden="true">
          <span className="design-streamline-node" />
          {showSegment && <span className="design-streamline-segment" />}
        </div>
        <div className={`design-dimension-card design-stage-shell ${active ? "is-active" : ""}`}>
          <div className="design-dimension-head">
            <div>
              <strong>{title}</strong>
              <p className="design-stage-subtitle">{subtitle}</p>
            </div>
            <div className="design-stage-toggle-meta">
              <span className="design-dimension-tag">{collapsed ? "collapsed" : "open"}</span>
              <span className={`collapsible-caret ${collapsed ? "is-collapsed" : ""}`}>▾</span>
            </div>
          </div>
          <div className="muted design-stage-description">{description}</div>
          <div className="design-stage-inline-summary">{summary}</div>
        </div>
      </button>
      {!collapsed && (
        <div className="design-stage-body">
          <div aria-hidden="true" />
          <div className="design-stage-content">{children}</div>
        </div>
      )}
    </article>
  );
}

export default function LayoutPlaceholderPanel({
  columns,
  datasetSummary,
  workloadSummary,
  onComparisonListChange,
  onGlobalLoadingStart,
  onGlobalLoadingEnd,
}: LayoutPlaceholderPanelProps) {
  const [selectedPartitionStrategy, setSelectedPartitionStrategy] = useState<string>("none");
  const [selectedPartitionColumns, setSelectedPartitionColumns] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [selectedLayoutTypes, setSelectedLayoutTypes] = useState<string[]>([
    layoutOptions[0].id,
  ]);
  const [selectedCandidateKeys, setSelectedCandidateKeys] = useState<string[]>([]);
  const [collapsed, setCollapsed] = useState(false);
  const [collapsedStages, setCollapsedStages] = useState<Record<DesignStageId, boolean>>({
    partition: false,
    layout: false,
  });
  const [latestRun, setLatestRun] = useState<LayoutEvaluation[]>([]);
  const [comparisonList, setComparisonList] = useState<LayoutEvaluation[]>([]);
  const [latestEvaluationMeta, setLatestEvaluationMeta] = useState<LayoutEvaluationResponse | null>(null);
  const [estimateLoading, setEstimateLoading] = useState(false);
  const [estimateError, setEstimateError] = useState<string | null>(null);
  const [latestRunSortDirection, setLatestRunSortDirection] = useState<ScoreSortDirection>("asc");
  const [comparisonListSortDirection, setComparisonListSortDirection] = useState<ScoreSortDirection>("asc");
  const [layoutSortKey, setLayoutSortKey] = useState<LayoutSortKey>("rangeShare");
  const [layoutSortDirection, setLayoutSortDirection] = useState<ScoreSortDirection>("desc");
  const getScoreValue = (item: LayoutEvaluation): number =>
    item.composite_score ?? item.avg_record_read_ratio;
  const sortEvaluationsByScore = (
    items: LayoutEvaluation[],
    direction: ScoreSortDirection,
  ): LayoutEvaluation[] => {
    const multiplier = direction === "asc" ? 1 : -1;
    return [...items].sort(
      (left, right) => (getScoreValue(left) - getScoreValue(right)) * multiplier,
    );
  };

  const toggleColumn = (column: string) => {
    setSelectedColumns((current) =>
      current.includes(column)
        ? current.filter((value) => value !== column)
        : [...current, column],
    );
  };

  const togglePartitionColumn = (column: string) => {
    setSelectedPartitionColumns((current) =>
      current.includes(column)
        ? current.filter((value) => value !== column)
        : [...current, column],
    );
  };

  const toggleLayoutType = (layoutType: string) => {
    setSelectedLayoutTypes((current) =>
      current.includes(layoutType)
        ? current.filter((value) => value !== layoutType)
        : [...current, layoutType],
    );
  };

  const toggleDesignStage = (stage: DesignStageId) => {
    setCollapsedStages((current) => ({
      ...current,
      [stage]: !current[stage],
    }));
  };

  const permutationLimitReached = useMemo(
    () => selectedColumns.length > 0 && factorial(selectedColumns.length, MAX_LAYOUT_PERMUTATIONS) > MAX_LAYOUT_PERMUTATIONS,
    [selectedColumns],
  );
  const selectedPhysicalLayoutCount = selectedLayoutTypes.filter(
    (layoutType) => layoutType !== "no layout",
  ).length;
  const totalCandidateCount =
    selectedCandidateKeys.length * selectedPhysicalLayoutCount
    + (selectedLayoutTypes.includes("no layout") ? 1 : 0);
  const sortedLatestRun = useMemo(
    () => sortEvaluationsByScore(latestRun, latestRunSortDirection),
    [latestRun, latestRunSortDirection],
  );
  const sortedComparisonList = useMemo(
    () => sortEvaluationsByScore(comparisonList, comparisonListSortDirection),
    [comparisonList, comparisonListSortDirection],
  );
  const columnProfileMap = useMemo(
    () => new Map((datasetSummary?.column_profiles ?? []).map((profile) => [profile.name, profile])),
    [datasetSummary?.column_profiles],
  );
  const columnTypeMap = useMemo(
    () => new Map((datasetSummary?.columns ?? []).map((column) => [column.name, column.inferred_type])),
    [datasetSummary?.columns],
  );
  const workloadFrequencyMap = useMemo(
    () => workloadSummary?.per_column_filter_frequency ?? {},
    [workloadSummary],
  );
  const avgPredicateSelectivityMap = useMemo(
    () => workloadSummary?.per_column_avg_predicate_selectivity ?? {},
    [workloadSummary],
  );
  const avgQuerySelectivityMap = useMemo(
    () => workloadSummary?.per_column_avg_query_selectivity ?? {},
    [workloadSummary],
  );
  const perColumnPredicateTypeMap = useMemo(
    () => workloadSummary?.per_column_predicate_type_distribution ?? {},
    [workloadSummary],
  );
  const columnColorMap = useMemo(
    () => buildCorrelationColorMap(datasetSummary?.correlation_summary ?? null),
    [datasetSummary?.correlation_summary],
  );
  const correlationStatsMap = useMemo(
    () => buildCorrelationStatsMap(datasetSummary?.correlation_summary ?? null),
    [datasetSummary?.correlation_summary],
  );
  const designColumns = useMemo(() => {
    const totalQueries = workloadSummary?.total_queries ?? 0;
    const rows: DesignColumnRow[] = columns
      .map((column) => {
        const profile = columnProfileMap.get(column);
        const nonNullCount = Math.max(
          (profile?.sample_size ?? 0) - (profile?.null_count ?? 0),
          0,
        );
        const distinctRatio = nonNullCount > 0
          ? ((profile?.distinct_count ?? 0) / nonNullCount) * 100
          : 0;
        const workloadFrequency = workloadFrequencyMap[column] ?? 0;
        const workloadFrequencyPercent = totalQueries > 0
          ? (workloadFrequency / totalQueries) * 100
          : 0;
        const predicateTypeCounts = perColumnPredicateTypeMap[column] ?? {};
        const equalityCount = predicateTypeCounts.equality ?? 0;
        const inListCount = predicateTypeCounts.in_list ?? 0;
        const rangeCount = predicateTypeCounts.range ?? 0;
        const equalityInShare = workloadFrequency > 0
          ? ((equalityCount + inListCount) / workloadFrequency) * 100
          : 0;
        const rangeShare = workloadFrequency > 0
          ? (rangeCount / workloadFrequency) * 100
          : 0;
        const avgPredicateSelectivity = avgPredicateSelectivityMap[column] ?? null;
        const avgQuerySelectivity = avgQuerySelectivityMap[column] ?? null;
        const correlationStats = correlationStatsMap.get(column) ?? {
          strength: 0,
          groupSize: 1,
        };
        return {
          name: column,
          inferredType: columnTypeMap.get(column) ?? "unknown",
          distinctRatio,
          workloadFrequency,
          workloadFrequencyPercent,
          equalityCount,
          inListCount,
          rangeCount,
          equalityInShare,
          rangeShare,
          avgPredicateSelectivity,
          avgQuerySelectivity,
          partitionRisk: buildPartitionRisk({
            distinctRatio,
            inferredType: columnTypeMap.get(column) ?? "unknown",
          }),
          partitionHint: buildPartitionHint({
            inferredType: columnTypeMap.get(column) ?? "unknown",
            workloadFrequencyPercent,
            distinctRatio,
            avgPredicateSelectivity,
            equalityInShare,
          }),
          orderingUtility: buildOrderingHint({
            workloadFrequencyPercent,
            distinctRatio,
            rangeShare,
            avgPredicateSelectivity,
            avgQuerySelectivity,
            correlationStrength: correlationStats.strength,
          }),
          correlationStrength: correlationStats.strength,
          correlationGroupSize: correlationStats.groupSize,
          correlationColor: columnColorMap.get(column) ?? null,
        };
      });

    return rows;
  }, [
    avgPredicateSelectivityMap,
    avgQuerySelectivityMap,
    columnColorMap,
    columnProfileMap,
    columnTypeMap,
    columns,
    correlationStatsMap,
    perColumnPredicateTypeMap,
    workloadFrequencyMap,
    workloadSummary?.total_queries,
  ]);
  const designColumnMap = useMemo(
    () => new Map(designColumns.map((column) => [column.name, column])),
    [designColumns],
  );
  const selectedLayoutRows = useMemo(
    () =>
      selectedColumns
        .map((column) => designColumnMap.get(column))
        .filter((column): column is DesignColumnRow => column !== undefined),
    [designColumnMap, selectedColumns],
  );
  const layoutCandidates = useMemo(
    () =>
      buildLayoutCandidates(
        selectedLayoutRows,
        MAX_LAYOUT_PERMUTATIONS,
        datasetSummary?.correlation_summary ?? null,
      ),
    [datasetSummary?.correlation_summary, selectedLayoutRows],
  );
  const selectedCandidates = useMemo(
    () =>
      layoutCandidates.filter((candidate) => selectedCandidateKeys.includes(candidate.key)),
    [layoutCandidates, selectedCandidateKeys],
  );
  const partitionColumns = useMemo(
    () => [...designColumns].sort(comparePartitionColumns),
    [designColumns],
  );
  const effectivePartitionColumns =
    selectedPartitionStrategy === "none" ? [] : selectedPartitionColumns;
  const selectedPartitionStrategyLabel = partitionStrategyOptions.find(
    (option) => option.id === selectedPartitionStrategy,
  )?.label ?? selectedPartitionStrategy;
  const partitionSpecLabel = formatPartitionSpec(
    selectedPartitionStrategy,
    effectivePartitionColumns,
  );
  const selectedLayoutTypeLabels = selectedLayoutTypes.map(
    (layoutType) => layoutOptions.find((option) => option.id === layoutType)?.label ?? layoutType,
  );
  const layoutEligibleColumns = useMemo(
    () => [...designColumns]
      .filter((column) => !effectivePartitionColumns.includes(column.name))
      .sort((left, right) => compareLayoutColumns(left, right, layoutSortKey, layoutSortDirection)),
    [
      designColumns,
      effectivePartitionColumns,
      layoutSortDirection,
      layoutSortKey,
    ],
  );

  useEffect(() => {
    setSelectedCandidateKeys((current) =>
      current.filter((key) => layoutCandidates.some((candidate) => candidate.key === key)),
    );
  }, [layoutCandidates]);

  useEffect(() => {
    setSelectedColumns((current) =>
      current.filter((column) => !effectivePartitionColumns.includes(column)),
    );
  }, [effectivePartitionColumns]);

  useEffect(() => {
    if (selectedPartitionStrategy === "none" && selectedPartitionColumns.length > 0) {
      setSelectedPartitionColumns([]);
    }
  }, [selectedPartitionColumns.length, selectedPartitionStrategy]);

  useEffect(() => {
    onComparisonListChange?.(comparisonList);
  }, [comparisonList, onComparisonListChange]);

  useEffect(() => {
    setLatestRun([]);
    setComparisonList([]);
    setLatestEvaluationMeta(null);
    setEstimateError(null);
    setLatestRunSortDirection("asc");
    setComparisonListSortDirection("asc");
    setLayoutSortKey("rangeShare");
    setLayoutSortDirection("desc");
  }, [datasetSummary?.dataset_id]);

  const toggleCandidate = (candidateKey: string) => {
    setSelectedCandidateKeys((current) =>
      current.includes(candidateKey)
        ? current.filter((value) => value !== candidateKey)
        : [...current, candidateKey],
    );
  };

  const canEstimate =
    selectedLayoutTypes.length > 0
    && (
      selectedCandidates.length > 0
      || selectedLayoutTypes.includes("no layout")
    )
    && Boolean(datasetSummary?.dataset_id);

  const handleEstimate = async () => {
    if (!datasetSummary?.dataset_id) {
      setEstimateError("Load a dataset before running layout estimation.");
      return;
    }

    if (!canEstimate) {
      setEstimateError("Select layout types and at least one permutation candidate.");
      return;
    }

    setEstimateLoading(true);
    setEstimateError(null);
    onGlobalLoadingStart?.("Running layout evaluation");

    try {
      const response = await evaluateLayout({
        dataset_id: datasetSummary.dataset_id,
        partition_strategy: selectedPartitionStrategy,
        partition_columns: effectivePartitionColumns,
        layout_types: selectedLayoutTypes,
        selected_candidates: selectedCandidates,
        include_query_estimates: false,
      });
      setLatestRun(response.evaluations);
      setLatestEvaluationMeta(response);
      setComparisonList((current) => {
        const merged = new Map(current.map((item) => [item.evaluation_id, item]));
        response.evaluations.forEach((item) => {
          merged.set(item.evaluation_id, item);
        });
        return Array.from(merged.values());
      });
    } catch (err) {
      setEstimateError(
        err instanceof Error ? err.message : "Layout estimation failed.",
      );
    } finally {
      setEstimateLoading(false);
      onGlobalLoadingEnd?.();
    }
  };

  const clearComparisonList = () => {
    setLatestRun([]);
    setComparisonList([]);
    setLatestEvaluationMeta(null);
    setEstimateError(null);
  };

  const toggleLatestRunSortDirection = () => {
    setLatestRunSortDirection((current) => (current === "asc" ? "desc" : "asc"));
  };

  const toggleComparisonListSortDirection = () => {
    setComparisonListSortDirection((current) => (current === "asc" ? "desc" : "asc"));
  };

  const toggleLayoutSort = (nextKey: LayoutSortKey) => {
    if (layoutSortKey === nextKey) {
      setLayoutSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setLayoutSortKey(nextKey);
    setLayoutSortDirection(
      nextKey === "name" || nextKey === "type" ? "asc" : "desc",
    );
  };

  const latestRunSortLabel = latestRunSortDirection === "asc" ? "low to high" : "high to low";
  const latestRunSortArrow = latestRunSortDirection === "asc" ? "↑" : "↓";
  const comparisonListSortLabel = comparisonListSortDirection === "asc" ? "low to high" : "high to low";
  const comparisonListSortArrow = comparisonListSortDirection === "asc" ? "↑" : "↓";
  const layoutSortArrow = layoutSortDirection === "asc" ? "↑" : "↓";

  return (
    <section className="panel">
      <CollapsibleHeader
        title="3. Physical Design Exploration"
        collapsed={collapsed}
        onToggle={() => setCollapsed((current) => !current)}
      />

      {!collapsed && (
        <>
          <div className="design-streamline">
            <DesignTimelineStage
              title="Partition Design"
              subtitle="Choose the outer physical split first"
              description={(
                <>
                  Use coarse-grained partition keys to skip large chunks of data first. This stage
                  combines the design dimension itself with the concrete partition strategy and
                  partition columns.
                </>
              )}
              summary={(
                <>
                  <span>Strategy: {selectedPartitionStrategyLabel}</span>
                  <span>Selected columns: {selectedPartitionColumns.length}</span>
                  <span>Active spec: {partitionSpecLabel}</span>
                </>
              )}
              collapsed={collapsedStages.partition}
              onToggle={() => toggleDesignStage("partition")}
              active={selectedPartitionStrategy !== "none" || selectedPartitionColumns.length > 0}
              showSegment
            >
              <div className="design-stage-section">
                <div className="design-stage-section-head">
                  <h4>Partition Strategy</h4>
                  <p className="muted">
                    Choose whether partitioning is part of this design spec before picking columns.
                  </p>
                </div>
                <label className="partition-switch" aria-label="Toggle partition strategy">
                  <span className="partition-switch-label">No Partition</span>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={selectedPartitionStrategy === "value"}
                    className={`partition-switch-control ${selectedPartitionStrategy === "value" ? "is-on" : ""}`}
                    onClick={() => setSelectedPartitionStrategy((current) => (current === "value" ? "none" : "value"))}
                  >
                    <span className="partition-switch-thumb" />
                  </button>
                  <span className="partition-switch-label">Value Partition</span>
                </label>
                <p className="muted design-stage-select-note">
                  {partitionStrategyOptions.find((option) => option.id === selectedPartitionStrategy)?.description}
                </p>
              </div>

              {selectedPartitionStrategy === "value" && (
                <div className="design-stage-section">
                  <div className="design-stage-section-head">
                    <h4>Partition Columns</h4>
                    <p className="muted">
                      This table emphasizes cardinality, equality/IN access, and partition risk.
                      Good partition columns are usually categorical and equality-heavy.
                    </p>
                  </div>
                  {columns.length === 0 ? (
                    <p className="muted">Load a dataset to populate columns.</p>
                  ) : (
                    <div className="table-wrap">
                      <table className="column-selection-table">
                        <thead>
                          <tr>
                            <th>Select</th>
                            <th>Column</th>
                            <th>Type</th>
                            <th>Cardinality Proxy</th>
                            <th>Equality + IN</th>
                            <th>Workload Freq</th>
                            <th>Partition Risk</th>
                            <th>Partition Hint</th>
                          </tr>
                        </thead>
                        <tbody>
                          {partitionColumns.map((column) => (
                            <tr
                              key={`partition-${column.name}`}
                              className={selectedPartitionColumns.includes(column.name) ? "is-selected" : ""}
                            >
                              <td>
                                <input
                                  type="checkbox"
                                  checked={selectedPartitionColumns.includes(column.name)}
                                  onChange={() => togglePartitionColumn(column.name)}
                                />
                              </td>
                              <td>
                                <strong>{column.name}</strong>
                              </td>
                              <td>{column.inferredType}</td>
                              <td>
                                <div className="table-metric">
                                  <span>{formatDistinctRatio(column.distinctRatio)}</span>
                                  <div className="mini-bar">
                                    <span style={{ width: `${Math.min(column.distinctRatio, 100)}%` }} />
                                  </div>
                                </div>
                              </td>
                              <td>
                                <div className="table-metric">
                                  <span>{column.equalityInShare.toFixed(1)}%</span>
                                  <div className="mini-bar mini-bar-equality">
                                    <span
                                      style={{
                                        width: `${Math.min(column.equalityInShare, 100)}%`,
                                      }}
                                    />
                                  </div>
                                  <small className="muted">
                                    eq {column.equalityCount} + in {column.inListCount}
                                  </small>
                                </div>
                              </td>
                              <td>
                                <div className="table-metric">
                                  <span>{column.workloadFrequencyPercent.toFixed(1)}%</span>
                                  <div className="mini-bar mini-bar-accent">
                                    <span
                                      style={{
                                        width: `${Math.min(column.workloadFrequencyPercent, 100)}%`,
                                      }}
                                    />
                                  </div>
                                </div>
                              </td>
                              <td>
                                <div
                                  className={`utility-hint utility-hint-${column.partitionRisk.level}`}
                                  title={column.partitionRisk.reason}
                                >
                                  <strong>{column.partitionRisk.label}</strong>
                                  <span>{column.partitionRisk.reason}</span>
                                </div>
                              </td>
                              <td>
                                <div
                                  className={`utility-hint utility-hint-${column.partitionHint.level}`}
                                  title={column.partitionHint.reason}
                                >
                                  <strong>{column.partitionHint.label}</strong>
                                  <span>{column.partitionHint.reason}</span>
                                </div>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                  <p className="muted">Selected partition columns: {selectedPartitionColumns.length}</p>
                  <p className="muted">Active partition spec: {partitionSpecLabel}</p>
                </div>
              )}
            </DesignTimelineStage>

            <DesignTimelineStage
              title="In-Partition Layout"
              subtitle="Choose ordering behavior inside the selected partitions"
              description={(
                <>
                  After fixing the partition design, choose layout columns, layout strategies, and
                  permutation candidates for within-partition pruning.
                </>
              )}
              summary={(
                <>
                  <span>Layout columns: {selectedColumns.length}</span>
                  <span>Layout types: {selectedLayoutTypes.length}</span>
                  <span>Permutations: {selectedCandidateKeys.length}</span>
                  <span>Total candidates: {totalCandidateCount}</span>
                </>
              )}
              collapsed={collapsedStages.layout}
              onToggle={() => toggleDesignStage("layout")}
              active={
                selectedColumns.length > 0
                || selectedCandidateKeys.length > 0
                || selectedLayoutTypes.some((layoutType) => layoutType !== "no layout")
              }
              showSegment={false}
            >
              <div className="design-stage-section">
                <div className="design-stage-section-head">
                  <h4>Layout Columns</h4>
                  <p className="muted">
                    This table emphasizes range pressure, selectivity, correlation, and ordering utility.
                  </p>
                </div>
                <div className="color-legend">
                  <span><i className="legend-chip legend-chip-neutral" /> no correlation group</span>
                  <span>
                    <i className="legend-chip legend-chip-group-a" />
                    <i className="legend-chip legend-chip-group-b" />
                    <i className="legend-chip legend-chip-group-c" />
                    grouped by strong correlation
                  </span>
                </div>
                <p className="muted">
                  When correlation has been computed, columns with the same dot color belong to the
                  same high-association group.
                </p>
                <p className="muted">
                  Lower selectivity means fewer sampled rows survive the predicate or full query,
                  so the column is usually more promising for pruning.
                </p>
                <p className="muted">
                  Utility hint combines workload frequency, selectivity, and distinct ratio into a
                  lightweight recommendation. It is only a guide, not a final decision.
                </p>
                {effectivePartitionColumns.length > 0 && (
                  <p className="muted">
                    Columns already chosen for partitioning are excluded here so layout selection
                    only works on the remaining columns.
                  </p>
                )}
                {columns.length === 0 ? (
                  <p className="muted">Load a dataset to populate columns.</p>
                ) : layoutEligibleColumns.length === 0 ? (
                  <p className="muted">
                    All available columns are currently assigned to partitioning. Remove one if you want
                    to design an in-partition layout.
                  </p>
                ) : (
                  <div className="table-wrap">
                    <table className="column-selection-table">
                      <thead>
                        <tr>
                          <th>Select</th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Sort by column name."
                              onClick={() => toggleLayoutSort("name")}
                            >
                              Column {layoutSortKey === "name" ? layoutSortArrow : ""}
                            </button>
                          </th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Sort by inferred data type."
                              onClick={() => toggleLayoutSort("type")}
                            >
                              Type {layoutSortKey === "type" ? layoutSortArrow : ""}
                            </button>
                          </th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Distinct ratio in the sampled non-null rows. Higher usually gives layout more room to separate nearby records."
                              onClick={() => toggleLayoutSort("distinctRatio")}
                            >
                              Cardinality Proxy {layoutSortKey === "distinctRatio" ? layoutSortArrow : ""}
                            </button>
                          </th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Share of this column's predicates that are range-like. Higher usually helps in-partition ordering."
                              onClick={() => toggleLayoutSort("rangeShare")}
                            >
                              Range Share {layoutSortKey === "rangeShare" ? layoutSortArrow : ""}
                            </button>
                          </th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Average selectivity of this column's own predicates on the current dataset sample. Lower is stronger."
                              onClick={() => toggleLayoutSort("avgPredicateSelectivity")}
                            >
                              Avg Predicate Sel. {layoutSortKey === "avgPredicateSelectivity" ? layoutSortArrow : ""}
                            </button>
                          </th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Average selectivity of full queries that include this column. Lower means this column tends to appear in stronger query patterns."
                              onClick={() => toggleLayoutSort("avgQuerySelectivity")}
                            >
                              Avg Query Sel. {layoutSortKey === "avgQuerySelectivity" ? layoutSortArrow : ""}
                            </button>
                          </th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Max observed correlation strength with another column in the correlation matrix."
                              onClick={() => toggleLayoutSort("correlationStrength")}
                            >
                              Correlation {layoutSortKey === "correlationStrength" ? layoutSortArrow : ""}
                            </button>
                          </th>
                          <th>
                            <button
                              type="button"
                              className="table-sort-button"
                              title="Ordering recommendation based on range usage, selectivity, cardinality, and correlation."
                              onClick={() => toggleLayoutSort("orderingUtility")}
                            >
                              Ordering Hint {layoutSortKey === "orderingUtility" ? layoutSortArrow : ""}
                            </button>
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {layoutEligibleColumns.map((column) => (
                          <tr
                            key={column.name}
                            className={`layout-column-row ${selectedColumns.includes(column.name) ? "is-selected" : ""}`}
                            style={
                              column.correlationColor
                                ? ({ ["--correlation-row-color"]: column.correlationColor } as CSSProperties)
                                : undefined
                            }
                          >
                            <td>
                              <input
                                type="checkbox"
                                checked={selectedColumns.includes(column.name)}
                                onChange={() => toggleColumn(column.name)}
                              />
                            </td>
                            <td>
                              <div className="column-name-cell">
                                <span
                                  className={`correlation-group-dot ${column.correlationColor ? "is-active" : ""}`}
                                  style={{
                                    backgroundColor: column.correlationColor ?? undefined,
                                  }}
                                  title={column.correlationColor ? "Grouped by strong correlation" : "No strong correlation group"}
                                />
                                <strong>{column.name}</strong>
                              </div>
                            </td>
                            <td>{column.inferredType}</td>
                            <td>
                              <div className="table-metric">
                                <span>{formatDistinctRatio(column.distinctRatio)}</span>
                                <div className="mini-bar">
                                  <span style={{ width: `${Math.min(column.distinctRatio, 100)}%` }} />
                                </div>
                              </div>
                            </td>
                            <td>
                              <div className="table-metric">
                                <span>{column.rangeShare.toFixed(1)}%</span>
                                <div className="mini-bar mini-bar-range">
                                  <span
                                    style={{
                                      width: `${Math.min(column.rangeShare, 100)}%`,
                                    }}
                                  />
                                </div>
                                <small className="muted">
                                  range {column.rangeCount}
                                </small>
                              </div>
                            </td>
                            <td>
                              <div className="table-metric">
                                <span>
                                  {column.avgPredicateSelectivity === null
                                    ? "-"
                                    : `${(column.avgPredicateSelectivity * 100).toFixed(1)}%`}
                                </span>
                                {column.avgPredicateSelectivity !== null && (
                                  <div className="mini-bar mini-bar-warning">
                                    <span
                                      style={{
                                        width: `${Math.min(column.avgPredicateSelectivity * 100, 100)}%`,
                                      }}
                                    />
                                  </div>
                                )}
                              </div>
                            </td>
                            <td>
                              <div className="table-metric">
                                <span>
                                  {column.avgQuerySelectivity === null
                                    ? "-"
                                    : `${(column.avgQuerySelectivity * 100).toFixed(1)}%`}
                                </span>
                                {column.avgQuerySelectivity !== null && (
                                  <div className="mini-bar mini-bar-query">
                                    <span
                                      style={{
                                        width: `${Math.min(column.avgQuerySelectivity * 100, 100)}%`,
                                      }}
                                    />
                                  </div>
                                )}
                              </div>
                            </td>
                            <td>
                              <div className="table-metric">
                                <span>{column.correlationStrength.toFixed(3)}</span>
                                <div className="mini-bar mini-bar-correlation">
                                  <span
                                    style={{
                                      width: `${Math.min(column.correlationStrength * 100, 100)}%`,
                                    }}
                                  />
                                </div>
                                <small className="muted">
                                  group size {column.correlationGroupSize}
                                </small>
                              </div>
                            </td>
                            <td>
                              <div
                                className={`utility-hint utility-hint-${column.orderingUtility.level}`}
                                title={column.orderingUtility.reason}
                              >
                                <strong>{column.orderingUtility.label}</strong>
                                <span>{column.orderingUtility.reason}</span>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              <div className="design-stage-stack">
                <div className="design-stage-section">
                  <div className="design-stage-section-head">
                    <h4>Layout Strategy</h4>
                    <p className="muted">
                      Compare no-layout, linear, z-order, and hilbert under the chosen partition design.
                    </p>
                  </div>
                  <div className="layout-option-grid">
                    {layoutOptions.map((option) => (
                      <label
                        key={option.id}
                        className={`layout-option-card ${selectedLayoutTypes.includes(option.id) ? "is-selected" : ""}`}
                      >
                        <input
                          type="checkbox"
                          checked={selectedLayoutTypes.includes(option.id)}
                          onChange={() => toggleLayoutType(option.id)}
                        />
                        <div className="layout-option-body">
                          <div className="layout-option-head">
                            <strong>{option.label}</strong>
                            <span className="layout-option-tag">{option.id}</span>
                          </div>
                          <LayoutTypePreview layoutType={option.id} />
                          <p className="muted">{option.description}</p>
                          {option.platforms.length > 0 && (
                            <div className="layout-platforms">
                              <span className="layout-platforms-label">Seen in</span>
                              <div className="layout-platform-badges">
                                {option.platforms.map((platform) => (
                                  <span
                                    key={`${option.id}-${platform.name}`}
                                    className={`layout-platform-badge layout-platform-badge-${platform.tone}`}
                                    title={platform.name}
                                    aria-label={platform.name}
                                  >
                                    <i>{platform.short}</i>
                                    <span>{platform.name}</span>
                                  </span>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      </label>
                    ))}
                  </div>
                </div>

                <div className="design-stage-section">
                  <div className="design-stage-section-head">
                    <h4>Permutation Candidates</h4>
                    <p className="muted permutation-hint">
                      Candidate orderings generated after choosing partition columns, then layout columns.
                      When correlation is available, ranking prefers orders that keep strongly
                      associated columns close together and earlier in the layout.
                    </p>
                  </div>
                  <div className="table-wrap">
                    {selectedColumns.length === 0 ? (
                      <p className="muted">Select layout columns above to generate permutations.</p>
                    ) : layoutCandidates.length === 0 ? (
                      <p className="muted">No candidates generated.</p>
                    ) : (
                      <table className="permutation-table">
                        <thead>
                          <tr>
                            <th>Select</th>
                            <th>Rank</th>
                            <th>Order</th>
                            <th>Lead Column</th>
                            <th>Estimated Utility</th>
                            <th>Corr Cohesion</th>
                          </tr>
                        </thead>
                        <tbody>
                          {layoutCandidates.map((candidate, index) => (
                            <tr
                              key={candidate.key}
                              className={selectedCandidateKeys.includes(candidate.key) ? "is-selected" : ""}
                            >
                              <td>
                                <input
                                  type="checkbox"
                                  checked={selectedCandidateKeys.includes(candidate.key)}
                                  onChange={() => toggleCandidate(candidate.key)}
                                />
                              </td>
                              <td>{index + 1}</td>
                              <td>
                                <div className="permutation-order">
                                  {candidate.columns.map((column, columnIndex) => (
                                    <span key={`${candidate.key}-${column}`} className="permutation-token">
                                      {column}
                                      {columnIndex < candidate.columns.length - 1 && (
                                        <i className="permutation-arrow">→</i>
                                      )}
                                    </span>
                                  ))}
                                </div>
                              </td>
                              <td>
                                <strong>{candidate.columns[0]}</strong>
                              </td>
                              <td>{candidate.estimatedUtilityScore.toFixed(2)}</td>
                              <td>{candidate.correlationCohesion.toFixed(3)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                  {permutationLimitReached && (
                    <p className="muted">
                      Showing the first {MAX_LAYOUT_PERMUTATIONS} permutations. Reduce selected
                      columns for full enumeration.
                    </p>
                  )}
                  <p className="muted">Selected permutation candidates: {selectedCandidateKeys.length}</p>
                  <p className="muted">Total candidates to try: {totalCandidateCount}</p>
                  <p className="muted">
                    Breakdown: {selectedCandidateKeys.length} permutations x {selectedPhysicalLayoutCount} layout
                    types
                    {selectedLayoutTypes.includes("no layout") ? " + 1 standalone no-layout case" : ""}
                  </p>
                  <p className="muted">Combined physical design spec: {partitionSpecLabel}</p>
                  {effectivePartitionColumns.length > 0 && (
                    <p className="muted">
                      Candidate counting is evaluated under the current partition spec, then layout
                      choices are applied within the touched partitions.
                    </p>
                  )}
                  {latestEvaluationMeta && (
                    <>
                      <p className="muted">
                        Evaluation basis: {latestEvaluationMeta.total_queries} queries, sample ratio{" "}
                        {(latestEvaluationMeta.sample_ratio * 100).toFixed(2)}%,{" "}
                        {latestEvaluationMeta.total_row_groups} simulated row groups.
                      </p>
                      <p className="muted">Partition spec in evaluation: {partitionSpecLabel}</p>
                      <p className="muted">Composite score is optional and cost-like: lower is better.</p>
                    </>
                  )}
                  <div className="sample-actions">
                    <button
                      type="button"
                      disabled={!canEstimate || estimateLoading}
                      onClick={() => {
                        void handleEstimate();
                      }}
                    >
                      Run Layout Evaluation
                    </button>
                    <button
                      type="button"
                      className="ghost-button"
                      disabled={comparisonList.length === 0}
                      onClick={clearComparisonList}
                    >
                      Clear Comparison List
                    </button>
                  </div>
                  <p className="muted">
                    The current backend uses real sample-pruning evaluation for `no layout` and
                    `linear`, and deterministic mock evaluation for `zorder` and `hilbert`.
                  </p>
                  {estimateError && <p className="error">{estimateError}</p>}
                </div>
              </div>
            </DesignTimelineStage>
          </div>

          <CollapsibleSubsection
            title="Current Design Summary"
            note={(
              <p className="muted">
                Roll-up of the partition and layout choices above. This is the physical design spec
                that will be sent into evaluation.
              </p>
            )}
          >
            <div className="design-rollup-grid">
              <article className="design-rollup-card">
                <h4>Partition</h4>
                <p><strong>Strategy:</strong> {selectedPartitionStrategyLabel}</p>
                <p><strong>Selected columns:</strong> {selectedPartitionColumns.length}</p>
                <p><strong>Effective spec:</strong> {partitionSpecLabel}</p>
              </article>
              <article className="design-rollup-card">
                <h4>Layout</h4>
                <p><strong>Columns:</strong> {selectedColumns.length > 0 ? selectedColumns.join(", ") : "None"}</p>
                <p><strong>Types:</strong> {selectedLayoutTypeLabels.join(", ")}</p>
                <p><strong>Permutations:</strong> {selectedCandidateKeys.length}</p>
              </article>
              <article className="design-rollup-card">
                <h4>Execution</h4>
                <p><strong>Total candidates:</strong> {totalCandidateCount}</p>
                <p><strong>Evaluation ready:</strong> {canEstimate ? "Yes" : "Not yet"}</p>
                <p>
                  <strong>Comparison list:</strong> {comparisonList.length}
                </p>
              </article>
            </div>
          </CollapsibleSubsection>

          {latestRun.length > 0 && (
            <CollapsibleSubsection
              title="Latest Run"
              note={<p className="muted">sorted by score, {latestRunSortLabel}</p>}
            >
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Partition Spec</th>
                      <th>Layout Type</th>
                      <th>Column Order</th>
                      <th>
                        <button
                          type="button"
                          className="table-sort-button"
                          onClick={toggleLatestRunSortDirection}
                        >
                          Score {latestRunSortArrow}
                        </button>
                      </th>
                      <th>Avg Record Ratio</th>
                      <th>Coverage ≥30%</th>
                      <th>Worst Query Ratio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedLatestRun.map((item) => (
                      <tr key={`latest-${item.evaluation_id}`}>
                        <td>{formatPartitionSpec(item.partition_strategy, item.partition_columns)}</td>
                        <td>{item.layout_type}</td>
                        <td>{item.layout_columns.length > 0 ? item.layout_columns.join(" -> ") : "-"}</td>
                        <td>{getScoreValue(item).toFixed(3)}</td>
                        <td>{(item.avg_record_read_ratio * 100).toFixed(1)}%</td>
                        <td>{(item.benefit_coverage_30 * 100).toFixed(1)}%</td>
                        <td>{(item.worst_query_read_ratio * 100).toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CollapsibleSubsection>
          )}

          {comparisonList.length > 0 && (
            <CollapsibleSubsection
              title="Comparison List"
              note={<p className="muted">sorted by score, {comparisonListSortLabel}</p>}
            >
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Rank</th>
                      <th>Partition Spec</th>
                      <th>Layout Type</th>
                      <th>Column Order</th>
                      <th>
                        <button
                          type="button"
                          className="table-sort-button"
                          onClick={toggleComparisonListSortDirection}
                        >
                          Score {comparisonListSortArrow}
                        </button>
                      </th>
                      <th>Avg Byte Ratio</th>
                      <th>Avg Row-Group Ratio</th>
                      <th>Coverage ≥30%</th>
                      <th>Estimator</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedComparisonList.map((item, index) => (
                      <tr key={item.evaluation_id}>
                        <td>{index + 1}</td>
                        <td>{formatPartitionSpec(item.partition_strategy, item.partition_columns)}</td>
                        <td>{item.layout_type}</td>
                        <td>{item.layout_columns.length > 0 ? item.layout_columns.join(" -> ") : "-"}</td>
                        <td>{getScoreValue(item).toFixed(3)}</td>
                        <td>{(item.avg_byte_read_ratio * 100).toFixed(1)}%</td>
                        <td>{(item.avg_row_group_read_ratio * 100).toFixed(1)}%</td>
                        <td>{(item.benefit_coverage_30 * 100).toFixed(1)}%</td>
                        <td>{item.algorithm}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="muted">
                Use the Verification section to select comparison candidates and run mock actual
                benchmark results.
              </p>
            </CollapsibleSubsection>
          )}
        </>
      )}
    </section>
  );
}

function formatDistinctRatio(value: number): string {
  return `${value.toFixed(DISTINCT_RATIO_DECIMALS)}%`;
}

function compareLayoutColumns(
  left: DesignColumnRow,
  right: DesignColumnRow,
  sortKey: LayoutSortKey,
  direction: ScoreSortDirection,
): number {
  const multiplier = direction === "asc" ? 1 : -1;

  const compareNullableNumber = (a: number | null, b: number | null, preferLower: boolean): number => {
    const safeA = a ?? (preferLower ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY);
    const safeB = b ?? (preferLower ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY);
    return safeA - safeB;
  };

  let result = 0;
  switch (sortKey) {
    case "name":
      result = left.name.localeCompare(right.name);
      break;
    case "type":
      result = left.inferredType.localeCompare(right.inferredType) || left.name.localeCompare(right.name);
      break;
    case "distinctRatio":
      result = left.distinctRatio - right.distinctRatio;
      break;
    case "rangeShare":
      result = left.rangeShare - right.rangeShare;
      break;
    case "avgPredicateSelectivity":
      result = compareNullableNumber(left.avgPredicateSelectivity, right.avgPredicateSelectivity, true);
      break;
    case "avgQuerySelectivity":
      result = compareNullableNumber(left.avgQuerySelectivity, right.avgQuerySelectivity, true);
      break;
    case "correlationStrength":
      result = left.correlationStrength - right.correlationStrength;
      break;
    case "orderingUtility":
      result = left.orderingUtility.score - right.orderingUtility.score;
      break;
    default:
      result = 0;
  }

  if (result === 0) {
    result = left.name.localeCompare(right.name);
  }
  return result * multiplier;
}

function comparePartitionColumns(left: DesignColumnRow, right: DesignColumnRow): number {
  const byHintScore = right.partitionHint.score - left.partitionHint.score;
  if (byHintScore !== 0) {
    return byHintScore;
  }

  const byRisk = left.partitionRisk.score - right.partitionRisk.score;
  if (byRisk !== 0) {
    return byRisk;
  }

  const byEqualityIn = right.equalityInShare - left.equalityInShare;
  if (byEqualityIn !== 0) {
    return byEqualityIn;
  }

  const byWorkload = right.workloadFrequencyPercent - left.workloadFrequencyPercent;
  if (byWorkload !== 0) {
    return byWorkload;
  }

  const byDistinctRatio = left.distinctRatio - right.distinctRatio;
  if (byDistinctRatio !== 0) {
    return byDistinctRatio;
  }

  return left.name.localeCompare(right.name);
}

function buildOrderingHint(input: {
  workloadFrequencyPercent: number;
  distinctRatio: number;
  rangeShare: number;
  avgPredicateSelectivity: number | null;
  avgQuerySelectivity: number | null;
  correlationStrength: number;
}): UtilityHint {
  const {
    workloadFrequencyPercent,
    distinctRatio,
    rangeShare,
    avgPredicateSelectivity,
    avgQuerySelectivity,
    correlationStrength,
  } = input;
  let score = 0;

  if (workloadFrequencyPercent >= 20) {
    score += 1.2;
  } else if (workloadFrequencyPercent >= 8) {
    score += 0.8;
  } else if (workloadFrequencyPercent >= 3) {
    score += 0.3;
  }

  if (rangeShare >= 65) {
    score += 2.2;
  } else if (rangeShare >= 35) {
    score += 1.4;
  } else if (rangeShare >= 15) {
    score += 0.6;
  }

  if (avgPredicateSelectivity !== null) {
    if (avgPredicateSelectivity <= 0.08) {
      score += 2.0;
    } else if (avgPredicateSelectivity <= 0.2) {
      score += 1.2;
    } else if (avgPredicateSelectivity <= 0.4) {
      score += 0.5;
    } else {
      score -= 0.6;
    }
  }

  if (avgQuerySelectivity !== null) {
    if (avgQuerySelectivity <= 0.03) {
      score += 1.2;
    } else if (avgQuerySelectivity <= 0.12) {
      score += 0.7;
    } else if (avgQuerySelectivity >= 0.45) {
      score -= 0.5;
    }
  }

  if (correlationStrength >= 0.72) {
    score += 1.0;
  } else if (correlationStrength >= 0.5) {
    score += 0.5;
  }

  if (distinctRatio >= 35) {
    score += 0.8;
  } else if (distinctRatio >= 8) {
    score += 0.4;
  } else if (distinctRatio <= 1.5) {
    score -= 0.7;
  }

  if (score >= 3.3) {
    return {
      level: "strong",
      label: "Strong",
      reason: "Good ordering candidate: strong range usage, selective predicates, or helpful correlation structure.",
      score,
    };
  }
  if (score >= 1.5) {
    return {
      level: "conditional",
      label: "Conditional",
      reason: "Can help ordering, but the benefit depends on query mix or companion columns.",
      score,
    };
  }
  return {
    level: "weak",
    label: "Weak",
    reason: "Low range pressure or weak pruning value for in-partition ordering.",
    score,
  };
}

function buildPartitionHint(input: {
  inferredType: string;
  workloadFrequencyPercent: number;
  distinctRatio: number;
  avgPredicateSelectivity: number | null;
  equalityInShare: number;
}): UtilityHint {
  const {
    inferredType,
    workloadFrequencyPercent,
    distinctRatio,
    avgPredicateSelectivity,
    equalityInShare,
  } = input;
  let score = 0;

  if (inferredType === "string") {
    score += 1.0;
  } else if (inferredType === "datetime") {
    score -= 0.2;
  } else if (inferredType === "float") {
    score -= 0.8;
  }

  if (workloadFrequencyPercent >= 12) {
    score += 1.8;
  } else if (workloadFrequencyPercent >= 5) {
    score += 1.0;
  } else if (workloadFrequencyPercent >= 2) {
    score += 0.4;
  }

  if (equalityInShare >= 70) {
    score += 2.0;
  } else if (equalityInShare >= 40) {
    score += 1.1;
  } else if (equalityInShare >= 20) {
    score += 0.4;
  } else {
    score -= 0.5;
  }

  if (distinctRatio <= 2.0) {
    score += 2.0;
  } else if (distinctRatio <= 8.0) {
    score += 1.3;
  } else if (distinctRatio <= 20.0) {
    score += 0.5;
  } else if (distinctRatio >= 45.0) {
    score -= 1.0;
  }

  if (avgPredicateSelectivity !== null) {
    if (avgPredicateSelectivity <= 0.1) {
      score += 1.4;
    } else if (avgPredicateSelectivity <= 0.25) {
      score += 0.8;
    } else if (avgPredicateSelectivity >= 0.6) {
      score -= 0.7;
    }
  }

  if (score >= 3.0) {
    return {
      level: "strong",
      label: "Strong",
      reason: "Looks suitable for partition pruning: low-to-moderate cardinality plus equality-heavy access patterns.",
      score,
    };
  }
  if (score >= 1.5) {
    return {
      level: "conditional",
      label: "Conditional",
      reason: "Could work as a partition key, but equality support or cardinality is only moderate.",
      score,
    };
  }
  return {
    level: "weak",
    label: "Weak",
    reason: "Likely too fine-grained, too range-heavy, or too weakly used for a strong partition key.",
    score,
  };
}

function buildPartitionRisk(input: {
  distinctRatio: number;
  inferredType: string;
}): PartitionRisk {
  const { distinctRatio, inferredType } = input;

  if (distinctRatio >= 45 || inferredType === "float") {
    return {
      level: "high",
      label: "High",
      reason: "Likely to create too many small partitions or unstable partition buckets.",
      score: 3,
    };
  }
  if (distinctRatio >= 12 || inferredType === "datetime") {
    return {
      level: "medium",
      label: "Medium",
      reason: "Usable, but watch for partition proliferation or uneven partition sizes.",
      score: 2,
    };
  }
  return {
    level: "low",
    label: "Low",
    reason: "Cardinality looks coarse enough for stable partition boundaries.",
    score: 1,
  };
}

function buildCorrelationColorMap(
  correlationSummary: CorrelationSummary | null,
): Map<string, string> {
  const colorMap = new Map<string, string>();
  if (!correlationSummary) {
    return colorMap;
  }

  const palette = [
    "rgba(31, 90, 166, 0.14)",
    "rgba(34, 139, 34, 0.14)",
    "rgba(184, 96, 36, 0.14)",
    "rgba(168, 62, 124, 0.14)",
    "rgba(120, 110, 36, 0.14)",
    "rgba(62, 132, 148, 0.14)",
  ];
  const threshold = 0.62;
  const columns = correlationSummary.columns;
  const visited = new Set<string>();
  let paletteIndex = 0;

  for (let rowIndex = 0; rowIndex < columns.length; rowIndex += 1) {
    const column = columns[rowIndex];
    if (visited.has(column)) {
      continue;
    }

    const group: string[] = [];
    const stack = [column];
    while (stack.length > 0) {
      const current = stack.pop();
      if (!current || visited.has(current)) {
        continue;
      }
      visited.add(current);
      group.push(current);

      const currentIndex = columns.indexOf(current);
      for (let colIndex = 0; colIndex < columns.length; colIndex += 1) {
        const score = correlationSummary.matrix[currentIndex]?.[colIndex] ?? null;
        if (score !== null && score >= threshold && current !== columns[colIndex]) {
          stack.push(columns[colIndex]);
        }
      }
    }

    if (group.length > 1) {
      const color = palette[paletteIndex % palette.length];
      group.forEach((groupColumn) => colorMap.set(groupColumn, color));
      paletteIndex += 1;
    }
  }

  return colorMap;
}

function buildCorrelationStatsMap(
  correlationSummary: CorrelationSummary | null,
): Map<string, { strength: number; groupSize: number }> {
  const stats = new Map<string, { strength: number; groupSize: number }>();
  if (!correlationSummary) {
    return stats;
  }

  const threshold = 0.62;
  const { columns, matrix } = correlationSummary;

  columns.forEach((column, rowIndex) => {
    let maxStrength = 0;
    let groupSize = 1;

    matrix[rowIndex]?.forEach((value, colIndex) => {
      if (rowIndex === colIndex || value === null) {
        return;
      }
      maxStrength = Math.max(maxStrength, value);
      if (value >= threshold) {
        groupSize += 1;
      }
    });

    stats.set(column, { strength: maxStrength, groupSize });
  });

  return stats;
}

function buildLayoutCandidates(
  columns: DesignColumnRow[],
  limit: number,
  correlationSummary: CorrelationSummary | null,
): LayoutCandidate[] {
  if (columns.length === 0) {
    return [];
  }

  const correlationMap = buildCorrelationPairMap(correlationSummary);
  const columnNames = columns.map((column) => column.name);
  const utilityMap = new Map(columns.map((column) => [column.name, column.orderingUtility.score]));
  const totalPermutations = factorial(columns.length, MAX_ENUMERATED_LAYOUT_PERMUTATIONS);

  if (totalPermutations <= MAX_ENUMERATED_LAYOUT_PERMUTATIONS) {
    return enumerateLayoutCandidates(columnNames, utilityMap, correlationMap).slice(0, limit);
  }

  return buildHeuristicLayoutCandidates(columnNames, utilityMap, correlationMap, limit);
}

function factorial(value: number, cap: number = MAX_LAYOUT_PERMUTATIONS): number {
  if (value <= 1) {
    return 1;
  }

  let product = 1;
  for (let current = 2; current <= value; current += 1) {
    product *= current;
    if (product > cap) {
      return product;
    }
  }
  return product;
}

function buildCorrelationPairMap(
  correlationSummary: CorrelationSummary | null,
): Map<string, number> {
  const pairMap = new Map<string, number>();
  if (!correlationSummary) {
    return pairMap;
  }

  const { columns, matrix } = correlationSummary;
  columns.forEach((left, rowIndex) => {
    matrix[rowIndex]?.forEach((value, colIndex) => {
      const right = columns[colIndex];
      if (rowIndex === colIndex || value === null) {
        return;
      }
      pairMap.set(getPairKey(left, right), value);
    });
  });
  return pairMap;
}

function getPairKey(left: string, right: string): string {
  return left < right ? `${left}\u0001${right}` : `${right}\u0001${left}`;
}

function getPairCorrelation(
  pairMap: Map<string, number>,
  left: string,
  right: string,
): number {
  return pairMap.get(getPairKey(left, right)) ?? 0;
}

function getPositionWeight(index: number): number {
  const weights = [1.8, 1.35, 1.0, 0.78, 0.62, 0.5, 0.42, 0.36];
  return weights[index] ?? 0.3;
}

function scoreLayoutOrder(
  orderedColumns: string[],
  utilityMap: Map<string, number>,
  correlationMap: Map<string, number>,
): { estimatedUtilityScore: number; correlationCohesion: number } {
  let score = 0;
  let adjacentCorrelation = 0;

  orderedColumns.forEach((column, index) => {
    score += (utilityMap.get(column) ?? 0) * getPositionWeight(index);

    if (index > 0) {
      const corr = getPairCorrelation(correlationMap, orderedColumns[index - 1], column);
      adjacentCorrelation += corr;
      score += corr * (index === 1 ? 1.35 : 1.0);
    }
  });

  return {
    estimatedUtilityScore: score,
    correlationCohesion: adjacentCorrelation / Math.max(orderedColumns.length - 1, 1),
  };
}

function enumerateLayoutCandidates(
  columns: string[],
  utilityMap: Map<string, number>,
  correlationMap: Map<string, number>,
): LayoutCandidate[] {
  const results: LayoutCandidate[] = [];
  const used = new Array(columns.length).fill(false);
  const current: string[] = [];

  const backtrack = () => {
    if (current.length === columns.length) {
      const metrics = scoreLayoutOrder(current, utilityMap, correlationMap);
      results.push({
        key: current.join("|"),
        columns: [...current],
        estimatedUtilityScore: metrics.estimatedUtilityScore,
        correlationCohesion: metrics.correlationCohesion,
      });
      return;
    }

    for (let index = 0; index < columns.length; index += 1) {
      if (used[index]) {
        continue;
      }

      used[index] = true;
      current.push(columns[index]);
      backtrack();
      current.pop();
      used[index] = false;
    }
  };

  backtrack();
  return results.sort(compareLayoutCandidates);
}

function buildHeuristicLayoutCandidates(
  columns: string[],
  utilityMap: Map<string, number>,
  correlationMap: Map<string, number>,
  limit: number,
): LayoutCandidate[] {
  type PartialCandidate = {
    columns: string[];
    remaining: string[];
  };

  const beamWidth = Math.min(Math.max(limit, 24), 48);
  let frontier: PartialCandidate[] = [{
    columns: [],
    remaining: [...columns],
  }];

  for (let depth = 0; depth < columns.length; depth += 1) {
    const next: Array<PartialCandidate & { score: number }> = [];

    frontier.forEach((candidate) => {
      candidate.remaining.forEach((column) => {
        const nextColumns = [...candidate.columns, column];
        const nextRemaining = candidate.remaining.filter((value) => value !== column);
        next.push({
          columns: nextColumns,
          remaining: nextRemaining,
          score: scoreLayoutOrder(nextColumns, utilityMap, correlationMap).estimatedUtilityScore,
        });
      });
    });

    next.sort((left, right) => {
      const byScore = right.score - left.score;
      if (byScore !== 0) {
        return byScore;
      }
      return left.columns.join("|").localeCompare(right.columns.join("|"));
    });

    frontier = next.slice(0, beamWidth).map(({ columns: nextColumns, remaining }) => ({
      columns: nextColumns,
      remaining,
    }));
  }

  return frontier
    .map((candidate) => {
      const metrics = scoreLayoutOrder(candidate.columns, utilityMap, correlationMap);
      return {
        key: candidate.columns.join("|"),
        columns: candidate.columns,
        estimatedUtilityScore: metrics.estimatedUtilityScore,
        correlationCohesion: metrics.correlationCohesion,
      };
    })
    .sort(compareLayoutCandidates)
    .slice(0, limit);
}

function compareLayoutCandidates(left: LayoutCandidate, right: LayoutCandidate): number {
  const byUtility = right.estimatedUtilityScore - left.estimatedUtilityScore;
  if (byUtility !== 0) {
    return byUtility;
  }

  const byCohesion = right.correlationCohesion - left.correlationCohesion;
  if (byCohesion !== 0) {
    return byCohesion;
  }

  return left.key.localeCompare(right.key);
}

function formatPartitionSpec(partitionStrategy: string, partitionColumns: string[]): string {
  if (partitionStrategy === "none" || partitionColumns.length === 0) {
    return "none";
  }
  return `${partitionStrategy}(${partitionColumns.join(", ")})`;
}

function LayoutTypePreview({ layoutType }: { layoutType: string }) {
  const points = getPreviewPoints(layoutType);
  const pathData = layoutType === "no layout"
    ? null
    : points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");

  return (
    <svg
      className="layout-preview"
      viewBox="0 0 120 120"
      role="img"
      aria-label={`${layoutType} ordering preview`}
    >
      <rect x="10" y="10" width="100" height="100" rx="10" className="layout-preview-frame" />
      {GRID_LINES.map((line) => (
        <line
          key={`v-${line}`}
          x1={line}
          y1={20}
          x2={line}
          y2={100}
          className="layout-preview-grid"
        />
      ))}
      {GRID_LINES.map((line) => (
        <line
          key={`h-${line}`}
          x1={20}
          y1={line}
          x2={100}
          y2={line}
          className="layout-preview-grid"
        />
      ))}
      {pathData && <path d={pathData} className="layout-preview-path" />}
      {points.map((point, index) => (
        <g key={`${layoutType}-${point.x}-${point.y}`}>
          <circle
            cx={point.x}
            cy={point.y}
            r={4.2}
            className={index === 0 ? "layout-preview-dot layout-preview-dot-start" : "layout-preview-dot"}
          />
          {(index === 0 || index === points.length - 1) && (
            <text x={point.x + 6} y={point.y - 6} className="layout-preview-label">
              {index === 0 ? "start" : "end"}
            </text>
          )}
        </g>
      ))}
    </svg>
  );
}

type PreviewPoint = {
  x: number;
  y: number;
};

const GRID_LINES = [40, 60, 80];
const GRID_POINT_COORDS = [30, 50, 70, 90];

function getPreviewPoints(layoutType: string): PreviewPoint[] {
  const gridPoints = GRID_POINT_COORDS.flatMap((y) =>
    GRID_POINT_COORDS.map((x) => ({ x, y })),
  );

  switch (layoutType) {
    case "linear":
      return gridPoints;
    case "zorder":
      return [0, 1, 4, 5, 2, 3, 6, 7, 8, 9, 12, 13, 10, 11, 14, 15].map(
        (index) => gridPoints[index],
      );
    case "hilbert":
      return [0, 4, 5, 1, 2, 3, 7, 6, 10, 11, 15, 14, 13, 9, 8, 12].map(
        (index) => gridPoints[index],
      );
    case "no layout":
    default:
      return [
        { x: 26, y: 34 },
        { x: 46, y: 88 },
        { x: 72, y: 27 },
        { x: 92, y: 62 },
        { x: 31, y: 71 },
        { x: 58, y: 48 },
        { x: 84, y: 93 },
        { x: 98, y: 41 },
      ];
  }
}
