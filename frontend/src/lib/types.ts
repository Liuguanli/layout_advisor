export type ColumnInfo = {
  name: string;
  inferred_type: string;
};

export type DistributionBucket = {
  label: string;
  count: number;
};

export type ColumnProfile = {
  name: string;
  inferred_type: string;
  sample_size: number;
  null_count: number;
  distinct_count: number;
  min_value: string | null;
  max_value: string | null;
  distribution_kind: string;
  distribution: DistributionBucket[];
};

export type CorrelationPair = {
  column_a: string;
  column_b: string;
  correlation: number;
  observation_count: number;
};

export type CorrelationSummary = {
  method: string;
  mode: string;
  columns: string[];
  column_kinds: Record<string, string>;
  matrix: Array<Array<number | null>>;
  top_pairs: CorrelationPair[];
};

export type DatasetSummary = {
  dataset_id: string | null;
  dataset_name: string | null;
  row_count: number;
  profile_sample_size: number;
  columns: ColumnInfo[];
  column_profiles: ColumnProfile[];
  correlation_summary: CorrelationSummary | null;
};

export type StaticDatasetItem = {
  dataset_id: string;
  name: string;
  file_path: string;
};

export type DatasetCatalogResponse = {
  datasets: StaticDatasetItem[];
};

export type WorkloadUploadResponse = {
  imported_queries: number;
  failed_queries: number;
};

export type StaticWorkloadItem = {
  workload_id: string;
  name: string;
  file_path: string;
};

export type WorkloadCatalogResponse = {
  workloads: StaticWorkloadItem[];
};

export type PairFrequency = {
  column_a: string;
  column_b: string;
  count: number;
};

export type PredicateCombinationFrequency = {
  columns: string[];
  count: number;
};

export type WorkloadSummary = {
  total_queries: number;
  predicate_type_distribution: Record<string, number>;
  per_column_filter_frequency: Record<string, number>;
  per_column_predicate_type_distribution: Record<string, Record<string, number>>;
  per_column_avg_predicate_selectivity: Record<string, number>;
  per_column_avg_query_selectivity: Record<string, number>;
  top_predicate_combinations: PredicateCombinationFrequency[];
  top_cooccurring_filter_pairs: PairFrequency[];
  query_complexity_distribution: Record<string, number>;
};

export type LayoutPermutationCandidate = {
  key: string;
  columns: string[];
};

export type LayoutEstimateRequest = {
  dataset_id: string | null;
  partition_strategy: string;
  partition_columns: string[];
  layout_types: string[];
  selected_candidates: LayoutPermutationCandidate[];
};

export type LayoutEstimateItem = {
  estimate_id: string;
  layout_type: string;
  candidate_key: string;
  column_order: string[];
  estimated_cost: number;
  algorithm: string;
  notes: string | null;
};

export type LayoutEstimateResponse = {
  dataset_id: string | null;
  workload_loaded: boolean;
  total_estimates: number;
  estimates: LayoutEstimateItem[];
};

export type QueryEstimate = {
  query_id: string;
  predicate_columns: string[];
  estimated_records_read: number;
  estimated_bytes_read: number;
  estimated_row_groups_read: number;
  benefit_vs_baseline: number;
};

export type ScoreWeights = {
  read_saving_weight: number;
  coverage_weight: number;
  worst_case_penalty_weight: number;
  layout_complexity_penalty_weight: number;
  num_columns_penalty_weight: number;
};

export type LayoutEvaluation = {
  evaluation_id: string;
  candidate_key: string;
  partition_strategy: string;
  partition_columns: string[];
  layout_type: string;
  layout_columns: string[];
  num_partition_columns: number;
  num_layout_columns: number;
  layout_complexity: number;
  query_estimates: QueryEstimate[];
  avg_record_read_ratio: number;
  avg_byte_read_ratio: number;
  avg_row_group_read_ratio: number;
  benefit_coverage_30: number;
  worst_query_read_ratio: number;
  composite_score: number | null;
  algorithm: string;
  notes: string | null;
};

export type LayoutEvaluationRequest = {
  dataset_id: string | null;
  partition_strategy: string;
  partition_columns: string[];
  layout_types: string[];
  selected_candidates: LayoutPermutationCandidate[];
  score_weights?: ScoreWeights | null;
  include_query_estimates?: boolean;
};

export type LayoutEvaluationResponse = {
  dataset_id: string | null;
  workload_loaded: boolean;
  total_queries: number;
  total_records: number;
  total_bytes: number;
  total_row_groups: number;
  sample_ratio: number;
  score_weights: ScoreWeights | null;
  evaluations: LayoutEvaluation[];
};

export type MockExecutionCandidate = {
  evaluation_id: string;
  partition_strategy: string;
  partition_columns: string[];
  layout_type: string;
  layout_columns: string[];
  estimated_score: number;
  avg_record_read_ratio: number;
  avg_row_group_read_ratio: number;
  layout_complexity: number;
};

export type MockExecutionRequest = {
  dataset_id: string | null;
  candidates: MockExecutionCandidate[];
};

export type MockExecutionResult = {
  evaluation_id: string;
  partition_strategy: string;
  partition_columns: string[];
  layout_type: string;
  layout_columns: string[];
  actual_runtime_ms: number;
  actual_records_read_ratio: number;
  actual_row_group_read_ratio: number;
  actual_score: number;
  runner: string;
  notes: string | null;
};

export type MockExecutionResponse = {
  dataset_id: string | null;
  total_results: number;
  results: MockExecutionResult[];
};
