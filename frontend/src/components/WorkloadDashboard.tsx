"use client";

import { useState } from "react";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { WorkloadSummary } from "../lib/types";
import CollapsibleHeader from "./CollapsibleHeader";
import CollapsibleSubsection from "./CollapsibleSubsection";

type WorkloadDashboardProps = {
  summary: WorkloadSummary | null;
  embedded?: boolean;
};

function toChartData(input: Record<string, number>): Array<{ name: string; value: number }> {
  return Object.entries(input).map(([name, value]) => ({ name, value }));
}

export default function WorkloadDashboard({
  summary,
  embedded = false,
}: WorkloadDashboardProps) {
  const [collapsed, setCollapsed] = useState(false);
  const containerClassName = embedded
    ? "subsection-block panel-subsection embedded-subsection"
    : "subsection-block panel panel-subsection";

  if (!summary) {
    return (
      <div className={containerClassName}>
        <CollapsibleHeader
          title="Workload Analysis"
          collapsed={collapsed}
          onToggle={() => setCollapsed((current) => !current)}
          level={3}
        />
        {!collapsed && (
          <div className="workload-analysis-content" style={{fontSize: 14, paddingLeft: 24, color: "grey"}}>
            <p>Load a static workload to see analysis metrics.</p>
          </div>
        )}
      </div>
    );
  }

  const predicateTypeData = toChartData(summary.predicate_type_distribution);
  const columnFreqData = toChartData(summary.per_column_filter_frequency).sort(
    (a, b) => b.value - a.value || a.name.localeCompare(b.name),
  );
  const complexityData = toChartData(summary.query_complexity_distribution).sort(
    (a, b) => Number(a.name) - Number(b.name),
  );
  const pairData = summary.top_cooccurring_filter_pairs.map((pair) => ({
    name: `${pair.column_a} + ${pair.column_b}`,
    value: pair.count,
  }));
  const shouldRotatePredicateAxis = predicateTypeData.length > 4
    || predicateTypeData.some((item) => item.name.length > 12);
  const predicateAxisTickFontSize = shouldRotatePredicateAxis
    ? 11
    : predicateTypeData.length <= 3
      ? 13
      : 12;
  const predicateAxisHeight = shouldRotatePredicateAxis ? 52 : 24;
  const predicateAxisBottomMargin = shouldRotatePredicateAxis ? 0 : 8;

  return (
    <div className={containerClassName}>
      <CollapsibleHeader
        title="Workload Analysis"
        collapsed={collapsed}
        onToggle={() => setCollapsed((current) => !current)}
        level={3}
      />
      {!collapsed && (
        <div className="workload-analysis-content">
          <CollapsibleSubsection title="Workload Totals">
            <p style={{marginTop: 0}}>
              <strong>Total queries:</strong> {summary.total_queries}
            </p>
          </CollapsibleSubsection>

          <div className="metrics-grid">
            <CollapsibleSubsection title="Predicate Type Distribution" className="chart-card">
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={predicateTypeData}
                    margin={{
                      top: 8,
                      right: 8,
                      bottom: predicateAxisBottomMargin,
                      left: 0,
                    }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="name"
                      interval={0}
                      angle={shouldRotatePredicateAxis ? -20 : 0}
                      textAnchor={shouldRotatePredicateAxis ? "end" : "middle"}
                      height={predicateAxisHeight}
                      tick={{ fontSize: predicateAxisTickFontSize }}
                    />
                    <YAxis allowDecimals={false} />
                    <Tooltip />
                    <Bar dataKey="value" fill="#1f77b4" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CollapsibleSubsection>
            <CollapsibleSubsection title="Query Complexity Distribution" className="chart-card">
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={complexityData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis allowDecimals={false} />
                    <Tooltip />
                    <Bar dataKey="value" fill="#ff7f0e" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CollapsibleSubsection>
          </div>

          <CollapsibleSubsection title="Per-Column Filter Frequency" className="chart-card chart-card-full">
            <div
              className="chart-wrap chart-wrap-tall"
              style={{ height: Math.max(320, columnFreqData.length * 34) }}
            >
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={columnFreqData}
                  layout="vertical"
                  margin={{ top: 8, right: 16, bottom: 8, left: 28 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" allowDecimals={false} />
                  <YAxis
                    type="category"
                    dataKey="name"
                    width={140}
                    tick={{ fontSize: 14 }}
                  />
                  <Tooltip />
                  <Bar dataKey="value" fill="#2ca02c" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CollapsibleSubsection>

          <CollapsibleSubsection title="Top Co-occurring Filter Pairs" className="chart-card chart-card-full">
            {summary.top_cooccurring_filter_pairs.length === 0 ? (
              <p>No filter pairs found.</p>
            ) : (
              <div className="chart-wrap chart-wrap-tall">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={pairData}
                    layout="vertical"
                    margin={{ top: 8, right: 16, bottom: 8, left: 28 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis type="number" allowDecimals={false} />
                    <YAxis
                      type="category"
                      dataKey="name"
                      width={180}
                      tick={{ fontSize: 14 }}
                    />
                    <Tooltip />
                    <Bar dataKey="value" fill="#0f8b8d">
                      {pairData.map((item) => (
                        <Cell key={item.name} fill="#0f8b8d" />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </CollapsibleSubsection>
        </div>
      )}
    </div>
  );
}
