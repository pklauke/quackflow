import datetime as dt

import duckdb
import pytest

from quackflow.window import register_window_functions_batch, register_window_functions_streaming


@pytest.fixture
def conn():
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE events (
            id INT,
            user_id VARCHAR,
            event_time TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO events VALUES
            (1, 'alice', TIMESTAMP '2024-01-01 10:01:00'),
            (2, 'bob',   TIMESTAMP '2024-01-01 10:06:00'),
            (3, 'alice', TIMESTAMP '2024-01-01 10:11:00'),
            (4, 'bob',   TIMESTAMP '2024-01-01 10:16:00'),
            (5, 'alice', TIMESTAMP '2024-01-01 10:21:00')
    """)
    return conn


class TestTumblingWindowStreaming:
    def test_filters_to_current_window(self, conn):
        register_window_functions_streaming(conn)
        conn.execute("SET VARIABLE __window_end = TIMESTAMP '2024-01-01 10:10:00'")

        result = conn.execute("""
            SELECT id, window_start, window_end
            FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
            ORDER BY id
        """).fetchall()

        assert len(result) == 2
        assert result[0][0] == 1
        assert result[1][0] == 2
        assert result[0][1] == dt.datetime(2024, 1, 1, 10, 0)
        assert result[0][2] == dt.datetime(2024, 1, 1, 10, 10)

    def test_different_window_sizes_same_end(self, conn):
        register_window_functions_streaming(conn)
        conn.execute("SET VARIABLE __window_end = TIMESTAMP '2024-01-01 10:20:00'")

        result_10min = conn.execute("""
            SELECT id FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
            ORDER BY id
        """).fetchall()

        result_5min = conn.execute("""
            SELECT id FROM tumbling_window('events', 'event_time', INTERVAL '5 minutes')
            ORDER BY id
        """).fetchall()

        assert [r[0] for r in result_10min] == [3, 4]
        assert [r[0] for r in result_5min] == [4]

    def test_window_end_is_exclusive(self, conn):
        register_window_functions_streaming(conn)
        conn.execute("INSERT INTO events VALUES (99, 'test', TIMESTAMP '2024-01-01 10:10:00')")
        conn.execute("SET VARIABLE __window_end = TIMESTAMP '2024-01-01 10:10:00'")

        result = conn.execute("""
            SELECT id FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
            WHERE id = 99
        """).fetchall()

        assert len(result) == 0

    def test_works_with_aggregation(self, conn):
        register_window_functions_streaming(conn)
        conn.execute("SET VARIABLE __window_end = TIMESTAMP '2024-01-01 10:10:00'")

        result = conn.execute("""
            SELECT window_start, window_end, user_id, COUNT(*) as cnt
            FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start, window_end, user_id
            ORDER BY user_id
        """).fetchall()

        assert len(result) == 2
        assert result[0][2] == "alice" and result[0][3] == 1
        assert result[1][2] == "bob" and result[1][3] == 1


class TestHoppingWindowStreaming:
    def test_filters_to_current_window(self, conn):
        register_window_functions_streaming(conn)
        conn.execute("SET VARIABLE __window_end = TIMESTAMP '2024-01-01 10:10:00'")

        result = conn.execute("""
            SELECT id, window_start, window_end
            FROM hopping_window('events', 'event_time', INTERVAL '10 minutes', INTERVAL '5 minutes')
            ORDER BY id
        """).fetchall()

        assert len(result) == 2
        assert result[0][0] == 1
        assert result[1][0] == 2
        assert result[0][1] == dt.datetime(2024, 1, 1, 10, 0)
        assert result[0][2] == dt.datetime(2024, 1, 1, 10, 10)


class TestTumblingWindowBatch:
    def test_one_window_per_record(self, conn):
        register_window_functions_batch(conn)
        conn.execute("SET VARIABLE __batch_start = TIMESTAMP '2024-01-01 10:00:00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMP '2024-01-01 10:30:00'")

        result = conn.execute("""
            SELECT id, COUNT(*) as window_count
            FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY id
        """).fetchall()

        for row in result:
            assert row[1] == 1

    def test_aggregation_per_window(self, conn):
        register_window_functions_batch(conn)
        conn.execute("SET VARIABLE __batch_start = TIMESTAMP '2024-01-01 10:00:00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMP '2024-01-01 10:30:00'")

        result = conn.execute("""
            SELECT window_start, window_end, COUNT(*) as cnt
            FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start, window_end
            ORDER BY window_start
        """).fetchall()

        assert len(result) == 3
        assert result[0][2] == 2
        assert result[1][2] == 2
        assert result[2][2] == 1


class TestHoppingWindowBatch:
    def test_expands_records_into_windows(self, conn):
        register_window_functions_batch(conn)
        conn.execute("SET VARIABLE __batch_start = TIMESTAMP '2024-01-01 10:00:00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMP '2024-01-01 10:30:00'")

        result = conn.execute("""
            SELECT id, window_start, window_end
            FROM hopping_window('events', 'event_time', INTERVAL '10 minutes', INTERVAL '5 minutes')
            WHERE id = 2
            ORDER BY window_start
        """).fetchall()

        assert len(result) == 2
        assert result[0][1] == dt.datetime(2024, 1, 1, 10, 0)
        assert result[0][2] == dt.datetime(2024, 1, 1, 10, 10)
        assert result[1][1] == dt.datetime(2024, 1, 1, 10, 5)
        assert result[1][2] == dt.datetime(2024, 1, 1, 10, 15)


class TestBatchStreamingEquivalence:
    def test_tumbling_window_equivalence(self, conn):
        size = dt.timedelta(minutes=10)
        batch_start = dt.datetime(2024, 1, 1, 10, 0)
        batch_end = dt.datetime(2024, 1, 1, 10, 30)

        register_window_functions_batch(conn)
        conn.execute(f"SET VARIABLE __batch_start = TIMESTAMP '{batch_start}'")
        conn.execute(f"SET VARIABLE __batch_end = TIMESTAMP '{batch_end}'")

        batch_result = conn.execute("""
            SELECT window_start, window_end, user_id, COUNT(*) as cnt
            FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start, window_end, user_id
            ORDER BY window_start, user_id
        """).fetchall()

        conn.execute("DROP FUNCTION tumbling_window")
        conn.execute("DROP FUNCTION hopping_window")
        register_window_functions_streaming(conn)

        streaming_results = []
        window_end = batch_start + size
        while window_end <= batch_end:
            conn.execute(f"SET VARIABLE __window_end = TIMESTAMP '{window_end}'")

            window_result = conn.execute("""
                SELECT window_start, window_end, user_id, COUNT(*) as cnt
                FROM tumbling_window('events', 'event_time', INTERVAL '10 minutes')
                GROUP BY window_start, window_end, user_id
                ORDER BY user_id
            """).fetchall()
            streaming_results.extend(window_result)
            window_end = window_end + size

        streaming_results.sort(key=lambda x: (x[0], x[2]))

        assert batch_result == streaming_results

    def test_hopping_window_equivalence(self, conn):
        size = dt.timedelta(minutes=10)
        hop = dt.timedelta(minutes=5)
        batch_start = dt.datetime(2024, 1, 1, 10, 0)
        batch_end = dt.datetime(2024, 1, 1, 10, 30)

        register_window_functions_batch(conn)
        conn.execute(f"SET VARIABLE __batch_start = TIMESTAMP '{batch_start}'")
        conn.execute(f"SET VARIABLE __batch_end = TIMESTAMP '{batch_end}'")

        batch_result = conn.execute("""
            SELECT window_start, window_end, COUNT(*) as cnt
            FROM hopping_window('events', 'event_time', INTERVAL '10 minutes', INTERVAL '5 minutes')
            GROUP BY window_start, window_end
            ORDER BY window_start
        """).fetchall()

        conn.execute("DROP FUNCTION tumbling_window")
        conn.execute("DROP FUNCTION hopping_window")
        register_window_functions_streaming(conn)

        streaming_results = []
        window_end = batch_start + size
        while window_end <= batch_end:
            conn.execute(f"SET VARIABLE __window_end = TIMESTAMP '{window_end}'")

            window_result = conn.execute("""
                SELECT window_start, window_end, COUNT(*) as cnt
                FROM hopping_window('events', 'event_time', INTERVAL '10 minutes', INTERVAL '5 minutes')
                GROUP BY window_start, window_end
            """).fetchall()
            streaming_results.extend(window_result)
            window_end = window_end + hop

        streaming_results.sort(key=lambda x: x[0])

        assert batch_result == streaming_results
