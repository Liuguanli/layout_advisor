"""SQL parsing helpers based on sqlglot for simple workload analysis."""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import expressions as exp


@dataclass(frozen=True)
class PredicateRecord:
    """Represents one extracted filter predicate from a query."""

    column: str
    predicate_type: str
    operator: str | None = None
    value_sql: str | None = None
    values_sql: tuple[str, ...] = ()
    lower_sql: str | None = None
    upper_sql: str | None = None
    lower_inclusive: bool | None = None
    upper_inclusive: bool | None = None
    prefix_value: str | None = None
    pattern_value: str | None = None


def parse_query_predicates(query: str) -> list[PredicateRecord]:
    """Parse a SQL query and return supported WHERE predicates.

    Supported predicate types:
    - equality (`=`)
    - inequality (`!=`, `<>`)
    - range (`>`, `>=`, `<`, `<=`, `BETWEEN`)
    - membership (`IN (...)`)
    - prefix (`LIKE 'abc%'`)
    - suffix (`LIKE '%abc'`)
    - contains (`LIKE '%abc%'`)
    - conjunctions (`AND`) are recursively flattened
    """

    try:
        parsed = sqlglot.parse_one(query)
    except Exception as exc:  # pragma: no cover - broad by design for parser failures
        raise ValueError(f"Failed to parse query: {exc}") from exc

    where_clause = parsed.args.get("where")
    if where_clause is None:
        return []

    predicates: list[PredicateRecord] = []
    for node in _flatten_and(where_clause.this):
        record = _classify_predicate(node)
        if record is not None:
            predicates.append(record)

    return predicates


def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
    """Flatten `AND` trees into a list of leaf predicates."""

    if isinstance(node, exp.Paren):
        inner = node.this
        if inner is None:
            return [node]
        return _flatten_and(inner)

    if isinstance(node, exp.And):
        left = node.args.get("this")
        right = node.args.get("expression")
        if left is None or right is None:
            return [node]
        return _flatten_and(left) + _flatten_and(right)
    return [node]


def _classify_predicate(node: exp.Expression) -> PredicateRecord | None:
    """Classify an expression into a supported predicate category."""

    if isinstance(node, exp.EQ):
        binary_parts = _extract_binary_parts(node)
        if binary_parts:
            column, value_expr, _ = binary_parts
            value_sql = _literal_sql(value_expr)
            if value_sql is not None:
                return PredicateRecord(
                    column=column,
                    predicate_type="equality",
                    operator="=",
                    value_sql=value_sql,
                )

    if isinstance(node, exp.NEQ):
        binary_parts = _extract_binary_parts(node)
        if binary_parts:
            column, value_expr, _ = binary_parts
            value_sql = _literal_sql(value_expr)
            if value_sql is not None:
                return PredicateRecord(
                    column=column,
                    predicate_type="not_equal",
                    operator="!=",
                    value_sql=value_sql,
                )

    if isinstance(node, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Between)):
        range_record = _build_range_predicate_record(node)
        if range_record is not None:
            return range_record

    if isinstance(node, exp.In):
        in_list_record = _build_in_list_predicate_record(node)
        if in_list_record is not None:
            return in_list_record

    if isinstance(node, exp.Like):
        like_record = _build_like_predicate_record(node)
        if like_record is not None:
            return like_record

    return None


def _extract_binary_parts(
    node: exp.Expression,
) -> tuple[str, exp.Expression, bool] | None:
    """Extract `(column, value_expression, column_on_left)` from a binary predicate."""

    left = node.args.get("this")
    right = node.args.get("expression")

    left_col = _column_name(left)
    right_col = _column_name(right)

    if left_col and right_col:
        return None
    if left_col and right is not None:
        return left_col, right, True
    if right_col and left is not None:
        return right_col, left, False
    return None


def _column_name(node: exp.Expression | None) -> str | None:
    """Return normalized column name if expression is a column."""

    if node is None:
        return None

    if isinstance(node, exp.Column):
        return node.name

    return None


def _extract_like_pattern(node: exp.Expression | None) -> str | None:
    """Extract raw LIKE pattern text from a string literal."""

    if not isinstance(node, exp.Literal) or not node.is_string:
        return None

    pattern = node.this
    if not isinstance(pattern, str):
        return None
    return pattern


def _classify_like_pattern(pattern: str) -> tuple[str, str] | None:
    """Map simple LIKE patterns to one supported predicate type."""

    if "_" in pattern:
        return None

    if pattern.endswith("%") and not pattern.startswith("%") and pattern.count("%") == 1:
        return "prefix", pattern[:-1]

    if pattern.startswith("%") and not pattern.endswith("%") and pattern.count("%") == 1:
        extracted = pattern[1:]
        return ("suffix", extracted) if extracted else None

    if pattern.startswith("%") and pattern.endswith("%") and pattern.count("%") == 2:
        extracted = pattern[1:-1]
        return ("contains", extracted) if extracted else None

    return None


def _build_like_predicate_record(node: exp.Like) -> PredicateRecord | None:
    """Build a record for supported LIKE patterns."""

    binary_parts = _extract_binary_parts(node)
    pattern_expr = node.args.get("expression")
    pattern = _extract_like_pattern(pattern_expr)
    if not binary_parts or pattern is None:
        return None

    classified = _classify_like_pattern(pattern)
    if classified is None:
        return None

    column, _, _ = binary_parts
    predicate_type, extracted = classified
    return PredicateRecord(
        column=column,
        predicate_type=predicate_type,
        operator="LIKE",
        value_sql=pattern_expr.sql(),
        prefix_value=extracted if predicate_type == "prefix" else None,
        pattern_value=extracted,
    )


def _literal_sql(node: exp.Expression | None) -> str | None:
    """Return raw SQL text for a literal-like value expression."""

    if node is None:
        return None
    if isinstance(node, exp.Column):
        return None
    if isinstance(node, exp.Cast):
        inner = node.args.get("this")
        target = node.args.get("to")
        if (
            isinstance(inner, exp.Literal)
            and inner.is_string
            and isinstance(target, exp.DataType)
            and target.this in {exp.DataType.Type.DATE, exp.DataType.Type.TIMESTAMP}
        ):
            return f"{target.this.value} {inner.sql()}"
    return node.sql()


def _build_in_list_predicate_record(node: exp.In) -> PredicateRecord | None:
    """Build a record for `column IN (literal, ...)` predicates."""

    base = node.args.get("this")
    column = _column_name(base)
    expressions = node.args.get("expressions") or []
    if column is None or not expressions:
        return None

    values_sql: list[str] = []
    for expression in expressions:
        literal_sql = _literal_sql(expression)
        if literal_sql is None:
            return None
        values_sql.append(literal_sql)

    return PredicateRecord(
        column=column,
        predicate_type="in_list",
        operator="IN",
        values_sql=tuple(values_sql),
    )


def _build_range_predicate_record(node: exp.Expression) -> PredicateRecord | None:
    """Build a range predicate with explicit lower/upper bound metadata."""

    if isinstance(node, exp.Between):
        base = node.args.get("this")
        lower = node.args.get("low")
        upper = node.args.get("high")
        column = _column_name(base)
        lower_sql = _literal_sql(lower)
        upper_sql = _literal_sql(upper)
        if column and lower_sql is not None and upper_sql is not None:
            return PredicateRecord(
                column=column,
                predicate_type="range",
                operator="BETWEEN",
                lower_sql=lower_sql,
                upper_sql=upper_sql,
                lower_inclusive=True,
                upper_inclusive=True,
            )
        return None

    binary_parts = _extract_binary_parts(node)
    if not binary_parts:
        return None
    column, value_expr, column_on_left = binary_parts
    value_sql = _literal_sql(value_expr)
    if value_sql is None:
        return None

    if isinstance(node, exp.GT):
        return PredicateRecord(
            column=column,
            predicate_type="range",
            operator=">",
            lower_sql=value_sql if column_on_left else None,
            upper_sql=None if column_on_left else value_sql,
            lower_inclusive=False if column_on_left else None,
            upper_inclusive=None if column_on_left else False,
        )
    if isinstance(node, exp.GTE):
        return PredicateRecord(
            column=column,
            predicate_type="range",
            operator=">=",
            lower_sql=value_sql if column_on_left else None,
            upper_sql=None if column_on_left else value_sql,
            lower_inclusive=True if column_on_left else None,
            upper_inclusive=None if column_on_left else True,
        )
    if isinstance(node, exp.LT):
        return PredicateRecord(
            column=column,
            predicate_type="range",
            operator="<",
            lower_sql=None if column_on_left else value_sql,
            upper_sql=value_sql if column_on_left else None,
            lower_inclusive=None if column_on_left else False,
            upper_inclusive=False if column_on_left else None,
        )
    if isinstance(node, exp.LTE):
        return PredicateRecord(
            column=column,
            predicate_type="range",
            operator="<=",
            lower_sql=None if column_on_left else value_sql,
            upper_sql=value_sql if column_on_left else None,
            lower_inclusive=None if column_on_left else True,
            upper_inclusive=True if column_on_left else None,
        )
    return None
