import datetime as dt

import sqlglot
from sqlglot import exp


def extract_hop_window_sizes(sql: str) -> list[dt.timedelta]:
    """Extract window sizes from HOP function calls in SQL."""
    parsed = sqlglot.parse_one(sql, dialect="duckdb")
    sizes: list[dt.timedelta] = []

    for func in parsed.find_all(exp.Anonymous):
        if func.name.upper() == "HOP" and len(func.expressions) >= 3:
            interval_arg = func.expressions[2]
            if isinstance(interval_arg, exp.Interval):
                size = _interval_to_timedelta(interval_arg)
                if size is not None:
                    sizes.append(size)

    return sizes


def _interval_to_timedelta(interval: exp.Interval) -> dt.timedelta | None:
    """Convert sqlglot Interval to timedelta."""
    unit = interval.unit
    if unit is None:
        return None

    unit_str = unit.this.upper() if hasattr(unit, "this") else str(unit).upper()
    value_expr = interval.this
    if not isinstance(value_expr, exp.Literal):
        return None

    value = int(value_expr.this)

    if unit_str in ("MINUTE", "MINUTES"):
        return dt.timedelta(minutes=value)
    elif unit_str in ("SECOND", "SECONDS"):
        return dt.timedelta(seconds=value)
    elif unit_str in ("HOUR", "HOURS"):
        return dt.timedelta(hours=value)
    elif unit_str in ("DAY", "DAYS"):
        return dt.timedelta(days=value)

    return None


def extract_tables(sql: str) -> set[str]:
    parsed = sqlglot.parse_one(sql, dialect="duckdb")
    tables: set[str] = set()

    for t in parsed.find_all(exp.Table):
        if t.name:
            tables.add(t.name)

    for func in parsed.find_all(exp.Anonymous):
        if func.expressions:
            first_arg = func.expressions[0]
            if isinstance(first_arg, exp.Literal) and first_arg.is_string:
                tables.add(first_arg.this)

    return tables
