"""Tests for richer SQL predicate parsing and evaluation."""

from __future__ import annotations

import unittest

import pandas as pd

from app.services.layout_score_v1 import predicate_mask_for_frame, query_to_column_ranges
from app.utils.sql_parser import parse_query_predicates


class SqlParserRichPredicateTest(unittest.TestCase):
    """Exercise richer supported predicate types beyond simple ranges."""

    def test_parse_complex_query_with_parentheses(self) -> None:
        predicates = parse_query_predicates(
            "SELECT l_shipmode, COUNT(*) FROM lineitem "
            "WHERE (l_shipmode IN ('AIR', 'RAIL', 'TRUCK')) "
            "AND (l_returnflag <> 'R' AND l_comment LIKE '%urgent%') "
            "AND l_shipinstruct LIKE '%RETURN' "
            "AND l_discount BETWEEN 0.02 AND 0.06 "
            "GROUP BY l_shipmode "
            "ORDER BY COUNT(*) DESC"
        )

        self.assertEqual(len(predicates), 5)
        self.assertEqual(
            [predicate.predicate_type for predicate in predicates],
            ["in_list", "not_equal", "contains", "suffix", "range"],
        )
        in_list = predicates[0]
        self.assertEqual(in_list.column, "l_shipmode")
        self.assertEqual(in_list.values_sql, ("'AIR'", "'RAIL'", "'TRUCK'"))

    def test_predicate_masks_for_richer_types(self) -> None:
        frame = pd.DataFrame(
            {
                "l_shipmode": ["AIR", "SHIP", "RAIL"],
                "l_returnflag": ["A", "R", "N"],
                "l_comment": [
                    "very urgent package",
                    "ordinary shipment",
                    "urgent spare part",
                ],
                "l_shipinstruct": [
                    "TAKE BACK RETURN",
                    "NONE",
                    "COLLECT COD RETURN",
                ],
                "l_discount": [0.03, 0.04, 0.07],
            }
        )
        predicates = parse_query_predicates(
            "SELECT * FROM lineitem "
            "WHERE l_shipmode IN ('AIR', 'RAIL') "
            "AND l_returnflag <> 'R' "
            "AND l_comment LIKE '%urgent%' "
            "AND l_shipinstruct LIKE '%RETURN' "
            "AND l_discount BETWEEN 0.02 AND 0.06"
        )

        mask = pd.Series(True, index=frame.index, dtype=bool)
        for predicate in predicates:
            predicate_mask = predicate_mask_for_frame(
                frame,
                predicate,
                {
                    "l_shipmode": "string",
                    "l_returnflag": "string",
                    "l_comment": "string",
                    "l_shipinstruct": "string",
                    "l_discount": "float",
                },
            )
            self.assertIsNotNone(predicate_mask)
            mask &= predicate_mask

        self.assertEqual(mask.tolist(), [True, False, False])

    def test_in_list_uses_range_envelope_for_pruning(self) -> None:
        predicates = parse_query_predicates(
            "SELECT * FROM lineitem "
            "WHERE l_shipmode IN ('AIR', 'RAIL', 'TRUCK') "
            "AND l_comment LIKE '%urgent%'"
        )

        query_ranges = query_to_column_ranges(
            predicates,
            {"l_shipmode": "string", "l_comment": "string"},
        )

        self.assertIn("l_shipmode", query_ranges)
        self.assertNotIn("l_comment", query_ranges)


if __name__ == "__main__":
    unittest.main()
