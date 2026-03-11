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
  const containerClassName = embedded ? "panel-subsection embedded-subsection" : "panel panel-subsection";

  if (!summary) {
    return (
      <div className={containerClassName}>
        <CollapsibleHeader
          title="Workload Analysis"
          collapsed={collapsed}
          onToggle={() => setCollapsed((current) => !current)}
          level={3}
        />
        {!collapsed && <p>Load a static workload to see analysis metrics.</p>}
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

  return (
    <div className={containerClassName}>
      <CollapsibleHeader
        title="Workload Analysis"
        collapsed={collapsed}
        onToggle={() => setCollapsed((current) => !current)}
        level={3}
      />
      {!collapsed && (
        <>
          <CollapsibleSubsection title="Workload Totals">
            <p>
              <strong>Total queries:</strong> {summary.total_queries}
            </p>
          </CollapsibleSubsection>

          <div className="metrics-grid">
            <CollapsibleSubsection title="Predicate Type Distribution" className="chart-card">
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={predicateTypeData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
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
                    tick={{ fontSize: 11 }}
                  />
                  <Tooltip />
                  <Bar dataKey="value" fill="#2ca02c" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CollapsibleSubsection>

          <CollapsibleSubsection title="Top Co-occurring Filter Pairs">
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
                      tick={{ fontSize: 11 }}
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
        </>
      )}
    </div>
  );
}
