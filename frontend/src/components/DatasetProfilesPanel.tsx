"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { ColumnProfile } from "../lib/types";
import CollapsibleSubsection from "./CollapsibleSubsection";

type DatasetProfilesPanelProps = {
  profiles: ColumnProfile[];
  sampleSize: number;
};

export default function DatasetProfilesPanel({
  profiles,
  sampleSize,
}: DatasetProfilesPanelProps) {
  const renderDistribution = (profile: ColumnProfile) => {
    if (profile.distribution_kind === "datetime_line") {
      return (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={profile.distribution}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="label"
              interval="preserveStartEnd"
              minTickGap={48}
              tick={{ fontSize: 11 }}
            />
            <YAxis allowDecimals={false} tick={{ fontSize: 11 }} />
            <Tooltip />
            <Line
              type="monotone"
              dataKey="count"
              stroke="#1f5aa6"
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      );
    }

    if (profile.distribution_kind === "line") {
      return (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={profile.distribution}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="label"
              interval={0}
              angle={-18}
              textAnchor="end"
              height={54}
              tick={{ fontSize: 11 }}
            />
            <YAxis allowDecimals={false} tick={{ fontSize: 11 }} />
            <Tooltip />
            <Line
              type="monotone"
              dataKey="count"
              stroke="#1f5aa6"
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      );
    }

    return (
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={profile.distribution}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="label"
            interval={0}
            angle={-20}
            textAnchor="end"
            height={56}
            tick={{ fontSize: 11 }}
          />
          <YAxis allowDecimals={false} tick={{ fontSize: 11 }} />
          <Tooltip />
          <Bar dataKey="count" fill="#1f5aa6" />
        </BarChart>
      </ResponsiveContainer>
    );
  };

  return (
    <CollapsibleSubsection
      title="Column Profiles"
      note={<p className="muted">Estimated from {sampleSize.toLocaleString()} sampled rows.</p>}
    >
      {profiles.length === 0 ? (
        <p className="muted">No column profiles selected. Choose one or more columns above.</p>
      ) : (
      <div className="profile-grid">
        {profiles.map((profile) => (
          <article key={profile.name} className="profile-card">
            <div className="profile-head">
              <div>
                <h4>{profile.name}</h4>
                <p className="muted">{profile.inferred_type}</p>
              </div>
              <div className="profile-stats">
                <span>distinct {profile.distinct_count}</span>
                <span>nulls {profile.null_count}</span>
              </div>
            </div>

            <div className="profile-range">
              <span>min {profile.min_value ?? "-"}</span>
              <span>max {profile.max_value ?? "-"}</span>
            </div>

            {profile.distribution.length === 0 ? (
              <p className="muted">No distribution available.</p>
            ) : (
              <div className="profile-chart">
                {renderDistribution(profile)}
              </div>
            )}
          </article>
        ))}
      </div>
      )}
    </CollapsibleSubsection>
  );
}
