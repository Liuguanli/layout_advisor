"use client";

import CollapsibleSubsection from "./CollapsibleSubsection";
import { CorrelationSummary } from "../lib/types";

type DatasetCorrelationPanelProps = {
  correlationSummary: CorrelationSummary | null;
  loading: boolean;
  error: string | null;
  onLoad: () => void;
};

const formatAssociation = (value: number | null) => {
  if (value === null) {
    return "N/A";
  }
  return value.toFixed(3);
};

const associationCellStyle = (value: number | null) => {
  if (value === null) {
    return { backgroundColor: "#f6f8fb", color: "#8a94a3" };
  }

  const alpha = Math.min(value, 1) * 0.82;
  return {
    backgroundColor: `rgba(31, 90, 166, ${0.10 + alpha * 0.58})`,
    color: value > 0.58 ? "#ffffff" : "#163a64",
  };
};

export default function DatasetCorrelationPanel({
  correlationSummary,
  loading,
  error,
  onLoad,
}: DatasetCorrelationPanelProps) {
  if (!correlationSummary) {
    return (
      <CollapsibleSubsection
        title="Correlation"
      >
        <div className="correlation-controls">
          <button type="button" onClick={onLoad} disabled={loading}>
            Compute Correlation
          </button>
        </div>
        <p className="muted">
          Correlation is computed on demand so dataset loading stays fast and stable.
        </p>
        {error && <p className="error">{error}</p>}
      </CollapsibleSubsection>
    );
  }

  const unsupportedColumns = correlationSummary.columns.filter(
    (column) => correlationSummary.column_kinds[column] === "unsupported_text",
  );

  return (
    <CollapsibleSubsection
      title="Correlation"
      note={(
        <>
          <p className="muted">
            Full-scan association matrix across all columns.
          </p>
          <p className="muted">
            The matrix uses a suitable association metric for each column pair so values remain
            comparable across numeric, date, and categorical columns.
          </p>
          {unsupportedColumns.length > 0 && (
            <p className="muted">
              High-cardinality text columns are still listed in the matrix, but unsupported
              pair scores stay `N/A`: {unsupportedColumns.join(", ")}.
            </p>
          )}
        </>
      )}
    >
      {error && <p className="error">{error}</p>}

      <div className="correlation-layout">
        <article className="correlation-card">
          <div className="section-header">
            <h3>Correlation Matrix</h3>
            <p className="muted">{correlationSummary.columns.length} columns</p>
          </div>

          <div className="matrix-legend">
            <span><i className="legend-chip legend-chip-neutral" /> N/A</span>
            <span><i className="legend-chip legend-chip-low" /> weak</span>
            <span><i className="legend-chip legend-chip-positive" /> strong</span>
          </div>

          <div className="matrix-wrap">
            <table className="matrix-table">
              <thead>
                <tr>
                  <th />
                  {correlationSummary.columns.map((column) => (
                    <th key={column} className="matrix-axis-top">{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {correlationSummary.columns.map((rowColumn, rowIndex) => (
                  <tr key={rowColumn}>
                    <th className="matrix-axis-side">{rowColumn}</th>
                    {correlationSummary.matrix[rowIndex].map((value, colIndex) => (
                      <td
                        key={`${rowColumn}-${correlationSummary.columns[colIndex]}`}
                        className="matrix-cell"
                        style={associationCellStyle(value)}
                        title={`${rowColumn} vs ${correlationSummary.columns[colIndex]}: ${formatAssociation(value)}`}
                      >
                        {formatAssociation(value)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      </div>
    </CollapsibleSubsection>
  );
}
