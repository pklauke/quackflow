"""
Physical plan optimization: Split logical SQL into two-stage execution plan.

Stage 1 (stateless): Source → Filter → Project → Repartition by GROUP BY keys
Stage 2 (stateful): Windowed aggregation → Sink

Approach: AST transformation using sqlglot
"""

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp


@dataclass
class HopInfo:
    source_table: str
    ts_column: str
    window_size: str
    alias: str | None
    containing_select: exp.Select | None


@dataclass
class StagePlan:
    sql: str
    ts_column: str
    source_table: str | None = None
    repartition_keys: list[str] | None = None
    window_size: str | None = None
    has_joins: bool = False
    base_tables: set[str] | None = None


@dataclass
class PhysicalPlan:
    stage1: StagePlan | list[StagePlan]
    stage2: StagePlan
    original_sql: str

    @property
    def stage1_plans(self) -> list[StagePlan]:
        if isinstance(self.stage1, list):
            return self.stage1
        return [self.stage1]


@dataclass
class SplitResult:
    success: bool
    plan: PhysicalPlan | None = None
    error: str | None = None
    warnings: list[str] = field(default_factory=list)


def _streaming_sets(hop_infos: list[HopInfo]) -> tuple[set[str], set[str]]:
    tables = {hop.source_table for hop in hop_infos}
    aliases = {hop.alias for hop in hop_infos if hop.alias}
    return tables, aliases


def _extract_ctes(ast: exp.Expression) -> dict[str, exp.CTE]:
    with_clause = ast.find(exp.With)
    if not with_clause:
        return {}
    return {cte.alias: cte for cte in with_clause.expressions if isinstance(cte, exp.CTE)}


def _find_cte_dependencies(cte_name: str, ctes: dict[str, exp.CTE]) -> list[exp.CTE]:
    if cte_name not in ctes:
        return []

    needed = []
    visited = set()

    def visit(name: str) -> None:
        if name in visited or name not in ctes:
            return
        visited.add(name)
        cte = ctes[name]
        for table in cte.find_all(exp.Table):
            visit(table.name)
        needed.append(cte)

    visit(cte_name)
    return needed


def _find_base_tables(cte_name: str, ctes: dict[str, exp.CTE]) -> set[str]:
    if cte_name not in ctes:
        return {cte_name}

    base_tables = set()
    visited = set()

    def visit(name: str) -> None:
        if name in visited:
            return
        visited.add(name)
        if name not in ctes:
            base_tables.add(name)
            return
        cte = ctes[name]
        for table in cte.find_all(exp.Table):
            visit(table.name)

    visit(cte_name)
    return base_tables


def _find_lookup_tables(ast: exp.Expression, streaming_tables: set[str], streaming_aliases: set[str]) -> set[str]:
    joins = list(ast.find_all(exp.Join))
    all_tables = {j.this.name for j in joins if isinstance(j.this, exp.Table)}
    all_aliases = {j.this.alias for j in joins if isinstance(j.this, exp.Table) and j.this.alias}
    return (all_tables - streaming_tables) | (all_aliases - streaming_aliases)


def _other_stream_refs(hop: HopInfo, hop_infos: list[HopInfo]) -> set[str]:
    return {h.alias for h in hop_infos if h.alias and h.alias != hop.alias} | {
        h.source_table for h in hop_infos if h.source_table != hop.source_table
    }


def _dedupe_conditions(conditions: list[exp.Expression]) -> exp.Expression | None:
    unique = list({c.sql(): c for c in conditions}.values())
    return _join_conditions(unique)


def _validate_stream_join_keys(join_keys: list[str], group_keys: list[str]) -> str | None:
    if not join_keys:
        return "Stream-stream join requires explicit JOIN ON condition with equality"
    if not set(group_keys).issubset(set(join_keys)):
        extra = set(group_keys) - set(join_keys)
        return f"GROUP BY keys {list(extra)} not in JOIN condition - would require additional shuffle"
    return None


def split_query(sql: str, stage1_output: str = "__stage1_input") -> SplitResult:
    try:
        ast = sqlglot.parse_one(sql, dialect="duckdb")
    except Exception as e:
        return SplitResult(success=False, error=f"Parse error: {e}")

    if error := _validate_query(ast):
        return SplitResult(success=False, error=error)

    hop_infos = _extract_hop_info(ast)
    if not hop_infos:
        return SplitResult(success=False, error="No HOP function found")

    group_by = ast.find(exp.Group)
    if group_by is None:
        return SplitResult(success=False, error="No GROUP BY clause (required for distribution)")

    ctes = _extract_ctes(ast)
    streaming_tables, streaming_aliases = _streaming_sets(hop_infos)
    non_streaming = _find_lookup_tables(ast, streaming_tables, streaming_aliases)
    joins = list(ast.find_all(exp.Join))

    if len(hop_infos) > 1:
        join_keys = _extract_join_repartition_keys(joins, streaming_tables, streaming_aliases)
        group_keys = _extract_repartition_keys(group_by, non_streaming)
        if error := _validate_stream_join_keys(join_keys, group_keys):
            return SplitResult(success=False, error=error)
        repartition_keys = join_keys
    else:
        repartition_keys = _extract_repartition_keys(group_by, non_streaming)

    if not repartition_keys:
        return SplitResult(success=False, error="No repartition keys found - need GROUP BY or JOIN condition")

    stage1_plans: list[StagePlan] = []
    stage2_ast = ast.copy()
    all_stage2_conditions: list[exp.Expression] = []

    for hop in hop_infos:
        output_name = f"{stage1_output}_{hop.source_table}" if len(hop_infos) > 1 else stage1_output
        other_streaming = _other_stream_refs(hop, hop_infos)

        columns_needed = (
            _collect_stage2_columns(hop.containing_select, non_streaming | other_streaming, hop.alias)
            if hop.containing_select
            else set()
        )
        columns_needed.add(hop.ts_column)

        hop_where = hop.containing_select.find(exp.Where) if hop.containing_select else None
        stage1_where, stage2_where, extra_cols = _split_where(hop_where, non_streaming, hop.alias, other_streaming)
        columns_needed.update(extra_cols)

        if stage2_where:
            all_stage2_conditions.extend(_flatten_and(stage2_where))

        cte_deps = _find_cte_dependencies(hop.source_table, ctes)
        base_tables = _find_base_tables(hop.source_table, ctes)

        stage1_plans.append(
            StagePlan(
                sql=_build_stage1_sql(hop.source_table, sorted(columns_needed), stage1_where, cte_deps),
                ts_column=hop.ts_column,
                source_table=hop.source_table,
                repartition_keys=repartition_keys,
                base_tables=base_tables,
            )
        )
        stage2_ast = _replace_source(stage2_ast, hop.source_table, output_name)

    primary_hop = hop_infos[0]
    stage2_ast = _update_where_in_select(
        stage2_ast, primary_hop.containing_select, _dedupe_conditions(all_stage2_conditions)
    )

    stage2 = StagePlan(
        sql=stage2_ast.sql(dialect="duckdb"),
        ts_column=primary_hop.ts_column,
        window_size=primary_hop.window_size,
        has_joins=len(joins) > 0,
    )

    stage1_result = stage1_plans[0] if len(stage1_plans) == 1 else stage1_plans
    return SplitResult(success=True, plan=PhysicalPlan(stage1=stage1_result, stage2=stage2, original_sql=sql))


def _validate_query(ast: exp.Expression) -> str | None:
    for subq in ast.find_all(exp.Subquery):
        if subq.find(exp.Group):
            return "Subqueries with GROUP BY not supported (no stacked aggregations)"
    return None


def _find_containing_select(node: exp.Expression | None) -> exp.Select | None:
    while node is not None:
        if isinstance(node, exp.Select):
            return node
        node = node.parent
    return None


def _extract_hop_info(ast: exp.Expression) -> list[HopInfo]:
    hops = []
    for func in ast.find_all(exp.Anonymous):
        if func.name.upper() == "HOP" and len(func.expressions) >= 3:
            source_arg, ts_arg, size_arg = func.expressions[:3]
            if isinstance(source_arg, exp.Literal) and source_arg.is_string:
                ts_column = ts_arg.this if isinstance(ts_arg, exp.Literal) else None
                alias = func.parent.alias if isinstance(func.parent, exp.Table) and func.parent.alias else None
                if ts_column:
                    hops.append(
                        HopInfo(
                            source_table=source_arg.this,
                            ts_column=ts_column,
                            window_size=size_arg.sql(dialect="duckdb"),
                            alias=alias,
                            containing_select=_find_containing_select(func.parent),
                        )
                    )
    return hops


def _extract_join_repartition_keys(
    joins: list[exp.Join],
    streaming_tables: set[str],
    streaming_aliases: set[str],
) -> list[str]:
    streaming = streaming_tables | streaming_aliases
    keys = []
    for join in joins:
        on_clause = join.args.get("on")
        if on_clause is None:
            continue
        for eq in on_clause.find_all(exp.EQ):
            left, right = eq.left, eq.right
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                continue
            left_streaming = left.table in streaming if left.table else False
            right_streaming = right.table in streaming if right.table else False
            if left_streaming and right_streaming:
                keys.append(left.name)
            elif left_streaming:
                keys.append(left.name)
            elif right_streaming:
                keys.append(right.name)
    return keys


def _is_window_column(name: str) -> bool:
    return name.lower() in {"window_start", "window_end"}


def _extract_repartition_keys(group_by: exp.Group, non_streaming: set[str]) -> list[str]:
    keys = []
    for expr in group_by.expressions:
        if isinstance(expr, exp.Column):
            if _is_window_column(expr.name):
                continue
            if expr.table and expr.table in non_streaming:
                continue
            keys.append(expr.name)
        else:
            expr_tables = {col.table for col in expr.find_all(exp.Column) if col.table}
            if expr_tables and all(t in non_streaming for t in expr_tables):
                continue
            keys.append(expr.sql(dialect="duckdb"))
    return keys


def _computed_aliases(select: exp.Select | None) -> set[str]:
    if not select:
        return set()
    return {
        expr.alias
        for expr in select.expressions
        if isinstance(expr, exp.Alias) and not isinstance(expr.this, exp.Column)
    }


def _collect_stage2_columns(
    ast: exp.Expression,
    non_streaming: set[str],
    source_alias: str | None,
) -> set[str]:
    columns = set()
    select = ast.find(exp.Select)

    for clause in [ast.find(exp.Group), select, ast.find(exp.Having)]:
        if clause:
            for col in clause.find_all(exp.Column):
                if _is_this_stream_column(col, non_streaming, source_alias):
                    columns.add(col.name)

    columns.discard("window_start")
    columns.discard("window_end")
    columns -= _computed_aliases(select)
    return columns


def _is_this_stream_column(col: exp.Column, non_streaming: set[str], source_alias: str | None) -> bool:
    if col.table:
        if col.table in non_streaming:
            return False
        if source_alias and col.table == source_alias:
            return True
        if source_alias and col.table != source_alias:
            return False
    return source_alias is None


def _classify_condition(
    cond: exp.Expression,
    non_streaming: set[str],
    source_alias: str | None,
    other_streaming: set[str],
) -> str:
    tables = _get_tables_in_expr(cond)
    refs_lookup = any(t in non_streaming for t in tables)
    refs_other = any(t in other_streaming for t in tables)
    refs_this = (source_alias and source_alias in tables) or not tables

    if refs_lookup:
        return "stage2"
    if refs_other and refs_this:
        return "stage2"
    if refs_other and not refs_this:
        return "skip"
    return "stage1"


def _local_columns(cond: exp.Expression, non_streaming: set[str], other_streaming: set[str]) -> set[str]:
    return {
        col.name
        for col in cond.find_all(exp.Column)
        if col.table not in non_streaming and col.table not in other_streaming
    }


def _split_where(
    where: exp.Where | None,
    non_streaming: set[str],
    source_alias: str | None,
    other_streaming_aliases: set[str] | None = None,
) -> tuple[exp.Expression | None, exp.Expression | None, set[str]]:
    if where is None:
        return None, None, set()

    other_streaming = other_streaming_aliases or set()
    stage1, stage2, extra = [], [], set()

    for cond in _flatten_and(where.this):
        dest = _classify_condition(cond, non_streaming, source_alias, other_streaming)
        if dest == "stage1":
            cond_copy = cond.copy()
            if source_alias:
                cond_copy = cond_copy.transform(_strip_alias, source_alias)
            stage1.append(cond_copy)
        elif dest == "stage2":
            stage2.append(cond)
            extra.update(_local_columns(cond, non_streaming, other_streaming))

    return _join_conditions(stage1), _join_conditions(stage2), extra


def _flatten_and(expr: exp.Expression) -> list[exp.Expression]:
    if isinstance(expr, exp.And):
        return _flatten_and(expr.left) + _flatten_and(expr.right)
    return [expr]


def _join_conditions(conditions: list[exp.Expression]) -> exp.Expression | None:
    if not conditions:
        return None
    result = conditions[0]
    for cond in conditions[1:]:
        result = exp.And(this=result, expression=cond)
    return result


def _get_tables_in_expr(expr: exp.Expression) -> set[str]:
    return {col.table for col in expr.find_all(exp.Column) if col.table}


def _strip_alias(node: exp.Expression, alias: str) -> exp.Expression:
    if isinstance(node, exp.Column) and node.table == alias:
        return exp.Column(this=node.this)
    return node


def _build_stage1_sql(
    source_table: str,
    columns: list[str],
    where_expr: exp.Expression | None,
    cte_deps: list[exp.CTE] | None = None,
) -> str:
    select = exp.Select(
        expressions=[exp.Column(this=exp.to_identifier(c)) for c in columns],
    ).from_(exp.Table(this=exp.to_identifier(source_table)))

    if where_expr:
        select = select.where(where_expr)

    if cte_deps:
        for cte in cte_deps:
            cte_sql = cte.this.sql(dialect="duckdb")
            select = select.with_(cte.alias, as_=cte_sql)

    return select.sql(dialect="duckdb")


def _replace_source(ast: exp.Expression, old_source: str, new_source: str) -> exp.Expression:
    def transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Anonymous) and node.name.upper() == "HOP":
            if len(node.expressions) >= 3:
                source_arg = node.expressions[0]
                if isinstance(source_arg, exp.Literal) and source_arg.this == old_source:
                    new_args = [exp.Literal.string(new_source)] + list(node.expressions[1:])
                    return exp.Anonymous(this="HOP", expressions=new_args)
        return node

    return ast.transform(transform)


def _has_hop_in_from(select: exp.Select) -> bool:
    from_clause = select.find(exp.From)
    if from_clause:
        for anon in from_clause.find_all(exp.Anonymous):
            if anon.name.upper() == "HOP":
                return True
    return False


def _update_where_in_select(
    ast: exp.Expression,
    target_select: exp.Select | None,
    new_where: exp.Expression | None,
) -> exp.Expression:
    if target_select is None:
        return ast

    def transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Select) and _has_hop_in_from(node):
            old_where = node.args.get("where")
            if old_where is None and new_where is None:
                return node
            if new_where is None and old_where is not None:
                node.set("where", None)
                return node
            if new_where is not None and old_where is not None:
                node.set("where", exp.Where(this=new_where))
                return node
        return node

    return ast.transform(transform)


# --- Demo / Testing ---

if __name__ == "__main__":
    test_queries = [
        # Simple aggregation
        """
        SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
        FROM HOP('events', 'event_time', INTERVAL '5 minutes')
        WHERE status = 'active'
        GROUP BY user_id, window_end
        """,
        # Multiple GROUP BY keys with HAVING
        """
        SELECT user_id, country, AVG(latency) as avg_latency
        FROM HOP('requests', 'request_time', INTERVAL '1 minute')
        WHERE endpoint = '/api/v1'
        GROUP BY user_id, country, window_end
        HAVING AVG(latency) > 100
        """,
        # Lookup JOIN
        """
        SELECT e.user_id, u.name, COUNT(*) as cnt
        FROM HOP('events', 'event_time', INTERVAL '5 minutes') e
        JOIN users u ON e.user_id = u.id
        GROUP BY e.user_id, u.name, window_end
        """,
        # WHERE clause splitting (AND with mixed tables)
        """
        SELECT e.user_id, u.name, COUNT(*) as cnt
        FROM HOP('events', 'event_time', INTERVAL '5 minutes') e
        JOIN users u ON e.user_id = u.id
        WHERE e.status = 'active' AND u.country = 'US' AND e.amount > 100
        GROUP BY e.user_id, u.name, window_end
        """,
        # OR condition (can't split - stays in Stage 2)
        """
        SELECT e.user_id, u.name, COUNT(*) as cnt
        FROM HOP('events', 'event_time', INTERVAL '5 minutes') e
        JOIN users u ON e.user_id = u.id
        WHERE e.status = 'active' OR u.country = 'US'
        GROUP BY e.user_id, u.name, window_end
        """,
        # Should fail: no GROUP BY
        """
        SELECT * FROM HOP('events', 'event_time', INTERVAL '5 minutes')
        WHERE status = 'active'
        """,
        # Should fail: stacked aggregation
        """
        SELECT * FROM (
            SELECT user_id, COUNT(*) as cnt
            FROM events GROUP BY user_id
        ) WHERE cnt > 10
        """,
        # Filter after groupby
        """
        WITH results AS (
            SELECT zone_id, window_end, COUNT(user_id) AS cnt
            FROM HOP('users', 'event_time', INTERVAL '5 minutes')
            GROUP BY zone_id, window_end
        )
        SELECT zone_id, window_end, cnt
        FROM results
        WHERE cnt > 100
        """,
        # deduplication
        """
        WITH last_event AS (
            SELECT 
                zone_id AS zone_id
                , window_end
                , user_id
                , ROW_NUMBER() OVER (PARTITION BY user_id, window_end ORDER BY event_time DESC) AS row_num
            FROM HOP('users', 'event_time', INTERVAL '5 minutes')
            QUALIFY row_num = 1
        )
        SELECT zone_id, window_end, COUNT(user_id) AS cnt
        FROM last_event
        GROUP BY zone_id, window_end
        """,
        # CTEs
        """
        WITH filtered_events AS (
            SELECT country, user_id, event_time
            FROM users
            WHERE country = 'us'
        ),
        last_events AS (
            SELECT country
            , window_end
            , user_id AS cnt
            , ROW_NUMBER() OVER (PARTITION BY user_id, window_end ORDER BY event_time DESC) AS row_num
            FROM HOP('filtered_events', 'event_time', INTERVAL '5 minutes')
        ),
        result AS (
            SELECT country, window_end, COUNT(user_id) AS cnt
            FROM last_events
            GROUP BY country, window_end
        )
        SELECT country, window_end, cnt FROM result
        """,
        # group by with expression
        """
        SELECT user_id, UPPER(status) as status_upper, COUNT(*) as cnt
        FROM HOP('events', 'event_time', INTERVAL '10 minutes')
        GROUP BY user_id, UPPER(status), window_end
        """,
        # stream-stream join
        """
        SELECT a.user_id, COUNT(*) as cnt
        FROM HOP('events', 'event_time', INTERVAL '10 minutes') a
        JOIN HOP('clicks', 'click_time', INTERVAL '10 minutes') b
        ON a.user_id = b.user_id AND a.window_end = b.window_end
        WHERE a.status = 'active' AND b.type = 'purchase'
        GROUP BY a.user_id, window_end
        """,
    ]

    for i, sql in enumerate(test_queries, 1):
        print(f"\n{'=' * 70}")
        print(f"Query {i}:")
        print(sql.strip())
        print("-" * 70)

        result = split_query(sql)

        if result.success:
            assert result.plan is not None
            print("✓ Split successful\n")
            for j, stage1 in enumerate(result.plan.stage1_plans, 1):
                if len(result.plan.stage1_plans) > 1:
                    print(f"Stage 1.{j} (stateless) - {stage1.source_table}:")
                else:
                    print("Stage 1 (stateless):")
                print(f"  SQL: {stage1.sql}")
                print(f"  Repartition keys: {stage1.repartition_keys}")
            print(f"\nStage 2 (stateful):")
            print(f"  SQL: {result.plan.stage2.sql}")
        else:
            print(f"✗ Failed: {result.error}")
