"""Unit tests for layout evaluation metrics."""

from __future__ import annotations

import unittest

from app.models.layout import QueryEstimate, ScoreWeights
from app.services.layout_service import (
    aggregate_layout_metrics,
    apply_benefit_against_baseline,
    compute_benefit_against_baseline,
    layout_complexity_for,
)


class LayoutMetricsTest(unittest.TestCase):
    """Verify metric aggregation and baseline benefit definitions."""

    def test_compute_benefit_against_baseline(self) -> None:
        self.assertAlmostEqual(
            compute_benefit_against_baseline(
                candidate_records_read=40,
                baseline_records_read=100,
            ),
            0.6,
        )
        self.assertEqual(
            compute_benefit_against_baseline(
                candidate_records_read=40,
                baseline_records_read=0,
            ),
            0.0,
        )

    def test_aggregate_layout_metrics(self) -> None:
        baseline = [
            QueryEstimate(
                query_id="q0001",
                predicate_columns=["l_shipdate"],
                estimated_records_read=1_000,
                estimated_bytes_read=100_000,
                estimated_row_groups_read=10,
                benefit_vs_baseline=0.0,
            ),
            QueryEstimate(
                query_id="q0002",
                predicate_columns=["l_discount", "l_quantity"],
                estimated_records_read=800,
                estimated_bytes_read=80_000,
                estimated_row_groups_read=8,
                benefit_vs_baseline=0.0,
            ),
        ]
        candidate = [
            QueryEstimate(
                query_id="q0001",
                predicate_columns=["l_shipdate"],
                estimated_records_read=350,
                estimated_bytes_read=35_000,
                estimated_row_groups_read=4,
                benefit_vs_baseline=0.0,
            ),
            QueryEstimate(
                query_id="q0002",
                predicate_columns=["l_discount", "l_quantity"],
                estimated_records_read=320,
                estimated_bytes_read=32_000,
                estimated_row_groups_read=3,
                benefit_vs_baseline=0.0,
            ),
        ]
        with_benefit = apply_benefit_against_baseline(candidate, baseline)
        evaluation = aggregate_layout_metrics(
            evaluation_id="linear::l_shipdate|l_discount",
            candidate_key="l_shipdate|l_discount",
            partition_strategy="none",
            partition_columns=[],
            layout_type="linear",
            layout_columns=["l_shipdate", "l_discount"],
            query_estimates=with_benefit,
            total_records=10_000,
            total_bytes=1_000_000,
            total_row_groups=20,
            score_weights=ScoreWeights(),
            max_layout_columns=3,
            algorithm="mock_query_metrics_v1",
            notes="test",
            include_query_estimates=True,
        )

        self.assertEqual(evaluation.layout_complexity, layout_complexity_for("linear"))
        self.assertEqual(evaluation.num_layout_columns, 2)
        self.assertAlmostEqual(evaluation.avg_record_read_ratio, 0.0335, places=4)
        self.assertAlmostEqual(evaluation.avg_byte_read_ratio, 0.0335, places=4)
        self.assertAlmostEqual(evaluation.avg_row_group_read_ratio, 0.175, places=4)
        self.assertAlmostEqual(evaluation.benefit_coverage_30, 1.0, places=4)
        self.assertAlmostEqual(evaluation.worst_query_read_ratio, 0.035, places=4)
        self.assertIsNotNone(evaluation.composite_score)


if __name__ == "__main__":
    unittest.main()
