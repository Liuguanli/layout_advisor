"""Generate a richer 1000-query TPC-H lineitem workload with mixed complexity."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import random
from typing import Callable


OUTPUT_PATH = Path(__file__).with_name("queries_rich_1000.txt")
QUERY_COUNT = 1000
SEED = 20260310

# Keep the workload mixed rather than overwhelmingly complex.
TARGET_COMPLEXITY_COUNTS = {
    1: 70,
    2: 110,
    3: 150,
    4: 180,
    5: 180,
    6: 140,
    7: 100,
    8: 70,
}

SHIPMODES = ["AIR", "AIR REG", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
SHIPINSTRUCTS = [
    "DELIVER IN PERSON",
    "COLLECT COD",
    "TAKE BACK RETURN",
    "NONE",
]
RETURNFLAGS = ["A", "N", "R"]
LINESTATUSES = ["F", "O"]
COMMENT_PREFIXES = ["special", "urgent", "quick", "final", "regular", "express"]
COMMENT_KEYWORDS = ["special", "urgent", "priority", "delayed", "fragile", "inspection"]

PredicateBuilder = Callable[[random.Random], str]
QueryRenderer = Callable[[random.Random, list[str]], str]


def random_date_window(rng: random.Random, min_days: int, max_days: int) -> tuple[str, str]:
    start_base = date(1992, 1, 1)
    max_offset = (date(1998, 12, 1) - start_base).days
    start = start_base + timedelta(days=rng.randint(0, max_offset))
    end = start + timedelta(days=rng.randint(min_days, max_days))
    return start.isoformat(), min(end, date(1998, 12, 31)).isoformat()


def fmt_date(value: str) -> str:
    return f"DATE '{value}'"


def sample_distinct(rng: random.Random, values: list[str], count: int) -> list[str]:
    return sorted(rng.sample(values, count))


def sql_list(values: list[str]) -> str:
    return ", ".join(values)


def int_list(rng: random.Random, low: int, high: int, count: int) -> list[int]:
    return sorted(rng.sample(range(low, high + 1), count))


def build_orderkey_equality(rng: random.Random) -> str:
    return f"l_orderkey = {rng.randint(1, 6_000_000)}"


def build_partkey_in(rng: random.Random) -> str:
    return f"l_partkey IN ({sql_list([str(value) for value in int_list(rng, 1, 200_000, 3)])})"


def build_suppkey_in(rng: random.Random) -> str:
    return f"l_suppkey IN ({sql_list([str(value) for value in int_list(rng, 1, 10_000, 2)])})"


def build_linenumber_in(rng: random.Random) -> str:
    return f"l_linenumber IN ({sql_list([str(value) for value in int_list(rng, 1, 7, 2)])})"


def build_shipdate_range(rng: random.Random) -> str:
    start, end = random_date_window(rng, 20, 150)
    return f"l_shipdate BETWEEN {fmt_date(start)} AND {fmt_date(end)}"


def build_commitdate_range(rng: random.Random) -> str:
    start, end = random_date_window(rng, 14, 110)
    return f"l_commitdate BETWEEN {fmt_date(start)} AND {fmt_date(end)}"


def build_receiptdate_range(rng: random.Random) -> str:
    start, end = random_date_window(rng, 20, 180)
    return f"l_receiptdate BETWEEN {fmt_date(start)} AND {fmt_date(end)}"


def build_quantity_range(rng: random.Random) -> str:
    low = rng.randint(1, 35)
    high = min(50, low + rng.randint(2, 10))
    return f"l_quantity BETWEEN {low} AND {high}"


def build_extendedprice_range(rng: random.Random) -> str:
    low = rng.randint(5_000, 85_000)
    high = low + rng.randint(8_000, 40_000)
    return f"l_extendedprice BETWEEN {low} AND {high}"


def build_discount_in(rng: random.Random) -> str:
    values = sorted(rng.sample([0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06], 3))
    return f"l_discount IN ({sql_list([f'{value:.2f}' for value in values])})"


def build_tax_range(rng: random.Random) -> str:
    low = rng.choice([0.00, 0.01, 0.02, 0.03])
    high = round(low + rng.choice([0.01, 0.02, 0.03]), 2)
    return f"l_tax BETWEEN {low:.2f} AND {high:.2f}"


def build_shipmode_equality(rng: random.Random) -> str:
    return f"l_shipmode = '{rng.choice(SHIPMODES)}'"


def build_returnflag_in(rng: random.Random) -> str:
    values = sample_distinct(rng, RETURNFLAGS, 2)
    return f"l_returnflag IN ({sql_list([repr(value) for value in values])})"


def build_linestatus_not_equal(rng: random.Random) -> str:
    return f"l_linestatus <> '{rng.choice(LINESTATUSES)}'"


def build_shipinstruct_suffix(rng: random.Random) -> str:
    suffix = rng.choice(["PERSON", "RETURN"])
    return f"l_shipinstruct LIKE '%{suffix}'"


def build_comment_pattern(rng: random.Random) -> str:
    pattern_type = rng.choice(["prefix", "contains"])
    token = rng.choice(COMMENT_PREFIXES if pattern_type == "prefix" else COMMENT_KEYWORDS)
    if pattern_type == "prefix":
        return f"l_comment LIKE '{token}%'"
    return f"l_comment LIKE '%{token}%'"


COLUMN_PREDICATES: dict[str, list[PredicateBuilder]] = {
    "l_orderkey": [build_orderkey_equality],
    "l_partkey": [build_partkey_in],
    "l_suppkey": [build_suppkey_in],
    "l_linenumber": [build_linenumber_in],
    "l_shipdate": [build_shipdate_range],
    "l_commitdate": [build_commitdate_range],
    "l_receiptdate": [build_receiptdate_range],
    "l_quantity": [build_quantity_range],
    "l_extendedprice": [build_extendedprice_range],
    "l_discount": [build_discount_in],
    "l_tax": [build_tax_range],
    "l_shipmode": [build_shipmode_equality],
    "l_returnflag": [build_returnflag_in],
    "l_linestatus": [build_linestatus_not_equal],
    "l_shipinstruct": [build_shipinstruct_suffix],
    "l_comment": [build_comment_pattern],
}


def render_projection_query(rng: random.Random, predicates: list[str]) -> str:
    select_list = rng.choice(
        [
            "l_orderkey, l_partkey, l_shipmode, l_shipdate, l_extendedprice",
            "l_orderkey, l_suppkey, l_quantity, l_discount, l_tax",
            "l_orderkey, l_linenumber, l_commitdate, l_receiptdate, l_shipinstruct",
        ]
    )
    order_by = rng.choice(
        [
            "l_shipdate DESC, l_orderkey",
            "l_extendedprice DESC, l_discount",
            "l_commitdate, l_orderkey, l_linenumber",
        ]
    )
    return (
        f"SELECT {select_list} "
        "FROM lineitem "
        f"WHERE {' AND '.join(f'({predicate})' for predicate in predicates)} "
        f"ORDER BY {order_by} "
        f"LIMIT {rng.randint(20, 120)};"
    )


def render_shipping_aggregate_query(rng: random.Random, predicates: list[str]) -> str:
    group_by = rng.choice(
        [
            ["l_shipmode"],
            ["l_shipmode", "l_returnflag"],
            ["l_shipmode", "l_linestatus"],
        ]
    )
    group_sql = ", ".join(group_by)
    return (
        f"SELECT {group_sql}, COUNT(*) AS row_count, SUM(l_extendedprice) AS revenue "
        "FROM lineitem "
        f"WHERE {' AND '.join(f'({predicate})' for predicate in predicates)} "
        f"GROUP BY {group_sql} "
        "ORDER BY revenue DESC, row_count DESC "
        f"LIMIT {rng.randint(8, 30)};"
    )


def render_value_profile_query(rng: random.Random, predicates: list[str]) -> str:
    return (
        "SELECT l_shipmode, l_linestatus, AVG(l_discount) AS avg_discount, "
        "MAX(l_extendedprice) AS max_price, MIN(l_quantity) AS min_qty "
        "FROM lineitem "
        f"WHERE {' AND '.join(f'({predicate})' for predicate in predicates)} "
        "GROUP BY l_shipmode, l_linestatus "
        "ORDER BY max_price DESC, avg_discount DESC;"
    )


def render_detail_query(rng: random.Random, predicates: list[str]) -> str:
    return (
        "SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, "
        "l_shipdate, l_commitdate, l_receiptdate "
        "FROM lineitem "
        f"WHERE {' AND '.join(f'({predicate})' for predicate in predicates)} "
        "ORDER BY l_receiptdate DESC, l_commitdate DESC "
        f"LIMIT {rng.randint(15, 80)};"
    )


def render_revenue_band_query(rng: random.Random, predicates: list[str]) -> str:
    return (
        "SELECT l_returnflag, l_shipmode, SUM(l_extendedprice) AS revenue, "
        "AVG(l_tax) AS avg_tax "
        "FROM lineitem "
        f"WHERE {' AND '.join(f'({predicate})' for predicate in predicates)} "
        "GROUP BY l_returnflag, l_shipmode "
        "ORDER BY revenue DESC, avg_tax DESC;"
    )


QUERY_RENDERERS: list[QueryRenderer] = [
    render_projection_query,
    render_shipping_aggregate_query,
    render_value_profile_query,
    render_detail_query,
    render_revenue_band_query,
]


def build_complexity_plan(rng: random.Random) -> list[int]:
    plan: list[int] = []
    for complexity, count in TARGET_COMPLEXITY_COUNTS.items():
        plan.extend([complexity] * count)
    if len(plan) != QUERY_COUNT:
        raise ValueError("TARGET_COMPLEXITY_COUNTS must sum to QUERY_COUNT")
    rng.shuffle(plan)
    return plan


def build_predicates(rng: random.Random, complexity: int) -> list[str]:
    selected_columns = rng.sample(list(COLUMN_PREDICATES), complexity)
    predicates = [rng.choice(COLUMN_PREDICATES[column])(rng) for column in selected_columns]
    rng.shuffle(predicates)
    return predicates


def build_queries() -> list[str]:
    rng = random.Random(SEED)
    complexity_plan = build_complexity_plan(rng)
    queries: list[str] = []
    for index, complexity in enumerate(complexity_plan):
        predicates = build_predicates(rng, complexity)
        renderer = QUERY_RENDERERS[index % len(QUERY_RENDERERS)]
        queries.append(renderer(rng, predicates))
    return queries


def main() -> None:
    OUTPUT_PATH.write_text("\n".join(build_queries()) + "\n", encoding="utf-8")
    print(f"Wrote {QUERY_COUNT} queries to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
