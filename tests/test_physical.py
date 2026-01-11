"""Test that 2-stage split produces identical results to original SQL."""

import duckdb
import pytest

from quackflow.physical import split_query
from quackflow.window import register_window_functions


@pytest.fixture
def conn():
    """Create DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")
    register_window_functions(conn)

    # Set window variables for batch mode (full range)
    conn.execute("SET VARIABLE __batch_start = TIMESTAMP '2024-01-01 00:00:00'")
    conn.execute("SET VARIABLE __batch_end = TIMESTAMP '2024-01-01 01:00:00'")
    conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

    # Create events table
    conn.execute("""
        CREATE TABLE events (
            user_id VARCHAR,
            status VARCHAR,
            amount DECIMAL,
            event_time TIMESTAMP
        )
    """)

    # Insert test data spanning multiple windows
    conn.execute("""
        INSERT INTO events VALUES
            ('alice', 'active', 100, TIMESTAMP '2024-01-01 00:05:00'),
            ('alice', 'active', 200, TIMESTAMP '2024-01-01 00:15:00'),
            ('bob', 'active', 150, TIMESTAMP '2024-01-01 00:08:00'),
            ('bob', 'inactive', 50, TIMESTAMP '2024-01-01 00:25:00'),
            ('alice', 'active', 300, TIMESTAMP '2024-01-01 00:35:00'),
            ('charlie', 'active', 250, TIMESTAMP '2024-01-01 00:12:00')
    """)

    # Create users lookup table
    conn.execute("""
        CREATE TABLE users (
            id VARCHAR,
            name VARCHAR,
            country VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO users VALUES
            ('alice', 'Alice Smith', 'US'),
            ('bob', 'Bob Jones', 'UK'),
            ('charlie', 'Charlie Brown', 'US')
    """)

    return conn


def run_two_stage(conn: duckdb.DuckDBPyConnection, original_sql: str) -> list:
    """Run query through 2-stage split and return results."""
    result = split_query(original_sql)
    assert result.success, f"Split failed: {result.error}"
    assert result.plan is not None

    # Run Stage 1 - create intermediate table
    stage1_sql = result.plan.stage1.sql
    conn.execute(f"CREATE TABLE __stage1_input AS {stage1_sql}")

    # Run Stage 2
    stage2_sql = result.plan.stage2.sql
    stage2_result = conn.execute(stage2_sql).fetchall()

    # Cleanup
    conn.execute("DROP TABLE __stage1_input")

    return sorted(stage2_result)


def run_original(conn: duckdb.DuckDBPyConnection, sql: str) -> list:
    """Run original SQL and return results."""
    return sorted(conn.execute(sql).fetchall())


class TestTwoStageEquivalence:
    def test_simple_aggregation(self, conn):
        sql = """
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_aggregation_with_where(self, conn):
        sql = """
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            WHERE status = 'active'
            GROUP BY user_id, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_aggregation_with_having(self, conn):
        sql = """
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            WHERE status = 'active'
            GROUP BY user_id, window_end
            HAVING SUM(amount) > 100
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_multiple_group_keys(self, conn):
        sql = """
            SELECT user_id, status, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, status, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_lookup_join(self, conn):
        sql = """
            SELECT e.user_id, u.name, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes') e
            JOIN users u ON e.user_id = u.id
            GROUP BY e.user_id, u.name, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_lookup_join_with_where_split(self, conn):
        sql = """
            SELECT e.user_id, u.name, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes') e
            JOIN users u ON e.user_id = u.id
            WHERE e.status = 'active' AND u.country = 'US'
            GROUP BY e.user_id, u.name, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_complex_aggregation(self, conn):
        sql = """
            SELECT user_id,
                   COUNT(*) * 2 as double_cnt,
                   SUM(amount) / COUNT(*) as avg_amount
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            WHERE status = 'active'
            GROUP BY user_id, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_cte_with_filter_after_aggregation(self, conn):
        """Filter on aggregation result should stay in Stage 2, not Stage 1."""
        sql = """
            WITH results AS (
                SELECT user_id, window_end, COUNT(*) AS cnt
                FROM HOP('events', 'event_time', INTERVAL '10 minutes')
                GROUP BY user_id, window_end
            )
            SELECT user_id, window_end, cnt
            FROM results
            WHERE cnt > 1
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_deduplication_with_row_number(self, conn):
        """ROW_NUMBER() alias should not be included in Stage 1 columns."""
        sql = """
            WITH last_event AS (
                SELECT
                    user_id,
                    amount,
                    window_end,
                    ROW_NUMBER() OVER (PARTITION BY user_id, window_end ORDER BY event_time DESC) AS row_num
                FROM HOP('events', 'event_time', INTERVAL '10 minutes')
                QUALIFY row_num = 1
            )
            SELECT user_id, window_end, SUM(amount) as total
            FROM last_event
            GROUP BY user_id, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_column_alias_same_name(self, conn):
        """Column aliased to same name (user_id AS user_id) should be in Stage 1."""
        sql = """
            SELECT user_id AS user_id, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

        # Verify user_id is in Stage 1 columns
        result = split_query(sql)
        assert "user_id" in result.plan.stage1.sql


class TestEdgeCases:
    """Test edge cases that could break the physical plan optimizer."""

    def test_qualify_clause_columns(self, conn):
        """Columns in QUALIFY clause should be in Stage 1."""
        sql = """
            WITH deduped AS (
                SELECT user_id, amount, window_end,
                       ROW_NUMBER() OVER (PARTITION BY user_id, window_end ORDER BY event_time DESC) AS rn
                FROM HOP('events', 'event_time', INTERVAL '10 minutes')
                QUALIFY rn = 1 AND amount > 50
            )
            SELECT user_id, window_end, SUM(amount) as total
            FROM deduped
            GROUP BY user_id, window_end
        """

        result = split_query(sql)
        assert result.success
        # amount should be in Stage 1 (referenced in QUALIFY)
        assert "amount" in result.plan.stage1.sql

    def test_order_by_columns(self, conn):
        """Columns in ORDER BY should be in Stage 1."""
        sql = """
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, window_end
            ORDER BY user_id, total
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage

    def test_group_by_with_expression(self, conn):
        """GROUP BY with expressions should include expression in repartition keys."""
        sql = """
            SELECT user_id, UPPER(status) as status_upper, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, UPPER(status), window_end
        """

        result = split_query(sql)
        assert result.success
        # Both user_id and UPPER(status) should be repartition keys
        assert "user_id" in result.plan.stage1.repartition_keys
        assert "UPPER(status)" in result.plan.stage1.repartition_keys


class TestCTEAsHopSource:
    """Test CTEs used as HOP source tables."""

    def test_cte_as_hop_source(self, conn):
        """CTE should be included in Stage 1 SQL."""
        sql = """
            WITH filtered AS (
                SELECT user_id, amount, event_time
                FROM events
                WHERE status = 'active'
            )
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
            FROM HOP('filtered', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, window_end
        """

        result = split_query(sql)
        assert result.success
        assert result.plan is not None

        # Stage 1 should include the CTE definition
        stage1_sql = result.plan.stage1.sql
        assert "WITH filtered AS" in stage1_sql
        assert "FROM filtered" in stage1_sql

        # base_tables should point to the actual source
        assert result.plan.stage1.base_tables == {"events"}

    def test_chained_ctes_as_hop_source(self, conn):
        """Chained CTEs should all be included in Stage 1."""
        sql = """
            WITH raw AS (
                SELECT user_id, status, amount, event_time FROM events
            ),
            filtered AS (
                SELECT user_id, amount, event_time
                FROM raw
                WHERE status = 'active'
            )
            SELECT user_id, COUNT(*) as cnt
            FROM HOP('filtered', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, window_end
        """

        result = split_query(sql)
        assert result.success
        assert result.plan is not None

        # Stage 1 should include both CTEs
        stage1_sql = result.plan.stage1.sql
        assert "WITH" in stage1_sql
        assert "raw AS" in stage1_sql
        assert "filtered AS" in stage1_sql

        # base_tables should point to the actual source
        assert result.plan.stage1.base_tables == {"events"}

    def test_cte_as_hop_source_equivalence(self, conn):
        """CTE-as-HOP-source should produce identical results."""
        # CTE exposes all columns needed for the outer query
        sql = """
            WITH prepared AS (
                SELECT user_id, amount, event_time
                FROM events
            )
            SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
            FROM HOP('prepared', 'event_time', INTERVAL '10 minutes')
            GROUP BY user_id, window_end
        """

        original = run_original(conn, sql)
        two_stage = run_two_stage(conn, sql)

        assert original == two_stage


class TestStreamStreamJoins:
    """Test stream-stream joins with multiple HOP functions."""

    def test_stream_stream_join_basic(self, conn):
        """Two streaming sources should produce two Stage 1 plans."""
        # Create second events table
        conn.execute("""
            CREATE TABLE events2 (
                user_id VARCHAR,
                action VARCHAR,
                event_time TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO events2 VALUES
                ('alice', 'click', TIMESTAMP '2024-01-01 00:05:00'),
                ('bob', 'view', TIMESTAMP '2024-01-01 00:15:00')
        """)

        sql = """
            SELECT a.user_id, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes') a
            JOIN HOP('events2', 'event_time', INTERVAL '10 minutes') b ON a.user_id = b.user_id
            GROUP BY a.user_id, window_end
        """

        result = split_query(sql)
        assert result.success
        assert result.plan is not None

        # Should have two Stage 1 plans
        stage1_plans = result.plan.stage1_plans
        assert len(stage1_plans) == 2

        # Each should have the right source table
        source_tables = {p.source_table for p in stage1_plans}
        assert source_tables == {"events", "events2"}

        # Both should repartition by user_id (from JOIN condition)
        for plan in stage1_plans:
            assert "user_id" in plan.repartition_keys

    def test_stream_stream_join_with_filters(self, conn):
        """WHERE filters should be split per stream."""
        conn.execute("""
            CREATE TABLE orders (
                user_id VARCHAR,
                status VARCHAR,
                order_time TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO orders VALUES
                ('alice', 'completed', TIMESTAMP '2024-01-01 00:05:00'),
                ('bob', 'pending', TIMESTAMP '2024-01-01 00:15:00')
        """)

        sql = """
            SELECT e.user_id, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes') e
            JOIN HOP('orders', 'order_time', INTERVAL '10 minutes') o ON e.user_id = o.user_id
            WHERE e.status = 'active' AND o.status = 'completed'
            GROUP BY e.user_id, window_end
        """

        result = split_query(sql)
        assert result.success
        assert result.plan is not None

        stage1_plans = result.plan.stage1_plans
        assert len(stage1_plans) == 2

        # Find the Stage 1 for events - should have status filter
        events_plan = next(p for p in stage1_plans if p.source_table == "events")
        assert "status = 'active'" in events_plan.sql

        # Find the Stage 1 for orders - should have status filter
        orders_plan = next(p for p in stage1_plans if p.source_table == "orders")
        assert "status = 'completed'" in orders_plan.sql

    def test_stream_stream_join_stage2_references_stage1_outputs(self, conn):
        """Stage 2 should reference the correct Stage 1 output tables."""
        conn.execute("""
            CREATE TABLE clicks (
                user_id VARCHAR,
                click_time TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO clicks VALUES ('alice', TIMESTAMP '2024-01-01 00:05:00')
        """)

        sql = """
            SELECT a.user_id, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes') a
            JOIN HOP('clicks', 'click_time', INTERVAL '10 minutes') b ON a.user_id = b.user_id
            GROUP BY a.user_id, window_end
        """

        result = split_query(sql)
        assert result.success
        assert result.plan is not None

        stage2_sql = result.plan.stage2.sql

        # Stage 2 should reference the stage1 output tables
        assert "__stage1_input_events" in stage2_sql
        assert "__stage1_input_clicks" in stage2_sql

        # Original table names should be replaced
        assert "HOP('events'" not in stage2_sql
        assert "HOP('clicks'" not in stage2_sql

    def test_stream_stream_join_rejects_mismatched_keys(self, conn):
        """GROUP BY keys not in JOIN condition should fail."""
        sql = """
            SELECT a.user_id, a.status, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes') a
            JOIN HOP('events', 'event_time', INTERVAL '10 minutes') b ON a.user_id = b.user_id
            GROUP BY a.user_id, a.status, window_end
        """

        result = split_query(sql)
        assert not result.success
        assert "GROUP BY keys" in result.error
        assert "additional shuffle" in result.error
