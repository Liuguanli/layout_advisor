"""Tests for score_v1 sample sorting and predicate range conversion."""

from __future__ import annotations

import unittest

import pandas as pd

from app.services.layout_score_v1 import (
    build_row_group_stats,
    parse_literal_value,
    query_to_column_ranges,
    row_group_matches_query,
    sort_sample_by_layout,
)
from app.utils.sql_parser import parse_query_predicates


class LayoutScoreV1Test(unittest.TestCase):
    """Exercise the real linear/no-layout estimator helpers."""

    def test_parse_query_ranges_with_dates_and_prefix(self) -> None:
        predicates = parse_query_predicates(
            "SELECT * FROM lineitem "
            "WHERE l_shipdate >= DATE '1995-01-31' "
            "AND l_shipdate < DATE '1995-05-03' "
            "AND l_comment LIKE 'special%'"
        )
        self.assertEqual(len(predicates), 3)
        date_predicates = [predicate for predicate in predicates if predicate.column == "l_shipdate"]
        self.assertEqual(len(date_predicates), 2)
        self.assertTrue(any(predicate.lower_sql == "DATE '1995-01-31'" for predicate in date_predicates))
        self.assertTrue(any(predicate.upper_sql == "DATE '1995-05-03'" for predicate in date_predicates))

        query_ranges = query_to_column_ranges(
            predicates,
            {"l_shipdate": "datetime", "l_comment": "string"},
        )
        self.assertIn("l_shipdate", query_ranges)
        self.assertIn("l_comment", query_ranges)
        self.assertIsNotNone(query_ranges["l_shipdate"].lower)
        self.assertIsNotNone(query_ranges["l_shipdate"].upper)
        self.assertFalse(query_ranges["l_shipdate"].upper_inclusive)
        self.assertFalse(query_ranges["l_comment"].upper_inclusive)

    def test_parse_cast_date_literal(self) -> None:
        parsed = parse_literal_value("CAST('1995-01-31' AS DATE)", "datetime")
        self.assertEqual(str(parsed.date()), "1995-01-31")

    def test_linear_sort_and_pruning(self) -> None:
        frame = pd.DataFrame(
            {
                "a": [3, 1, 2, 4],
                "b": [30, 10, 20, 40],
            }
        )
        sorted_frame = sort_sample_by_layout(
            frame,
            layout_type="linear",
            layout_columns=["a"],
            column_types={"a": "integer", "b": "integer"},
        )
        self.assertEqual(sorted_frame["a"].tolist(), [1, 2, 3, 4])

        row_groups = build_row_group_stats(
            sorted_frame,
            filter_columns=["a"],
            column_types={"a": "integer"},
            rows_per_group_sample=2,
        )
        predicates = parse_query_predicates("SELECT * FROM t WHERE a >= 3")
        query_ranges = query_to_column_ranges(predicates, {"a": "integer"})
        self.assertEqual(len(row_groups), 2)
        self.assertFalse(row_group_matches_query(row_groups[0], query_ranges))
        self.assertTrue(row_group_matches_query(row_groups[1], query_ranges))


if __name__ == "__main__":
    unittest.main()
