import sqlglot
from sqlglot import exp


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
