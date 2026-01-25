import datetime as dt

import duckdb
import pytest

from quackflow._internal.window import register_window_functions


@pytest.fixture
def conn():
    conn = duckdb.connect(":memory:")
    register_window_functions(conn)

    conn.execute("""
        CREATE TABLE events (
            id INT,
            user_id VARCHAR,
            event_time TIMESTAMPTZ
        )
    """)
    conn.execute("""
        INSERT INTO events VALUES
            (1, 'alice', TIMESTAMPTZ '2024-01-01 10:01:00+00'),
            (2, 'bob',   TIMESTAMPTZ '2024-01-01 10:06:00+00'),
            (3, 'alice', TIMESTAMPTZ '2024-01-01 10:11:00+00'),
            (4, 'bob',   TIMESTAMPTZ '2024-01-01 10:16:00+00'),
            (5, 'alice', TIMESTAMPTZ '2024-01-01 10:21:00+00')
    """)
    return conn


class TestWindowStreaming:
    def test_filters_to_current_window(self, conn):
        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:00:00+00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '2024-01-01 10:10:00+00'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

        result = conn.execute("""
            SELECT id, window_start, window_end
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            ORDER BY id
        """).fetchall()

        assert len(result) == 2
        assert result[0][0] == 1
        assert result[1][0] == 2
        assert result[0][1].astimezone(dt.timezone.utc) == dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
        assert result[0][2].astimezone(dt.timezone.utc) == dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc)

    def test_different_window_sizes_same_end(self, conn):
        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:10:00+00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '2024-01-01 10:20:00+00'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

        result_10min = conn.execute("""
            SELECT id FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            ORDER BY id
        """).fetchall()

        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:15:00+00'")
        result_5min = conn.execute("""
            SELECT id FROM HOP('events', 'event_time', INTERVAL '5 minutes')
            ORDER BY id
        """).fetchall()

        assert [r[0] for r in result_10min] == [3, 4]
        assert [r[0] for r in result_5min] == [4]

    def test_window_end_is_exclusive(self, conn):
        conn.execute("INSERT INTO events VALUES (99, 'test', TIMESTAMPTZ '2024-01-01 10:10:00+00')")
        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:00:00+00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '2024-01-01 10:10:00+00'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

        result = conn.execute("""
            SELECT id FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            WHERE id = 99
        """).fetchall()

        assert len(result) == 0

    def test_works_with_aggregation(self, conn):
        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:00:00+00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '2024-01-01 10:10:00+00'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

        result = conn.execute("""
            SELECT window_start, window_end, user_id, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start, window_end, user_id
            ORDER BY user_id
        """).fetchall()

        assert len(result) == 2
        assert result[0][2] == "alice" and result[0][3] == 1
        assert result[1][2] == "bob" and result[1][3] == 1


class TestWindowBatch:
    def test_tumbling_one_window_per_record(self, conn):
        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:00:00+00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '2024-01-01 10:30:00+00'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

        result = conn.execute("""
            SELECT id, COUNT(*) as window_count
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY id
        """).fetchall()

        for row in result:
            assert row[1] == 1

    def test_tumbling_aggregation_per_window(self, conn):
        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:00:00+00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '2024-01-01 10:30:00+00'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

        result = conn.execute("""
            SELECT window_start, window_end, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start, window_end
            ORDER BY window_start
        """).fetchall()

        assert len(result) == 3
        assert result[0][2] == 2
        assert result[1][2] == 2
        assert result[2][2] == 1

    def test_hopping_expands_records_into_windows(self, conn):
        conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '2024-01-01 10:00:00+00'")
        conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '2024-01-01 10:30:00+00'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '5 minutes'")

        result = conn.execute("""
            SELECT id, window_start, window_end
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            WHERE id = 2
            ORDER BY window_start
        """).fetchall()

        assert len(result) == 2
        assert result[0][1].astimezone(dt.timezone.utc) == dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
        assert result[0][2].astimezone(dt.timezone.utc) == dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc)
        assert result[1][1].astimezone(dt.timezone.utc) == dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc)
        assert result[1][2].astimezone(dt.timezone.utc) == dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc)


class TestBatchStreamingEquivalence:
    def test_tumbling_window_equivalence(self, conn):
        size = dt.timedelta(minutes=10)
        hop = size
        batch_start = dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
        batch_end = dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc)

        conn.execute(f"SET VARIABLE __batch_start = TIMESTAMPTZ '{batch_start.isoformat()}'")
        conn.execute(f"SET VARIABLE __batch_end = TIMESTAMPTZ '{batch_end.isoformat()}'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '10 minutes'")

        batch_result = conn.execute("""
            SELECT window_start, window_end, user_id, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start, window_end, user_id
            ORDER BY window_start, user_id
        """).fetchall()

        streaming_results = []
        window_start = batch_start
        window_end = batch_start + size
        while window_end <= batch_end:
            conn.execute(f"SET VARIABLE __batch_start = TIMESTAMPTZ '{window_start.isoformat()}'")
            conn.execute(f"SET VARIABLE __batch_end = TIMESTAMPTZ '{window_end.isoformat()}'")

            window_result = conn.execute("""
                SELECT window_start, window_end, user_id, COUNT(*) as cnt
                FROM HOP('events', 'event_time', INTERVAL '10 minutes')
                GROUP BY window_start, window_end, user_id
                ORDER BY user_id
            """).fetchall()
            streaming_results.extend(window_result)
            window_start = window_start + hop
            window_end = window_end + hop

        streaming_results.sort(key=lambda x: (x[0], x[2]))

        assert batch_result == streaming_results

    def test_hopping_window_equivalence(self, conn):
        size = dt.timedelta(minutes=10)
        hop = dt.timedelta(minutes=5)
        batch_start = dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
        batch_end = dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc)

        conn.execute(f"SET VARIABLE __batch_start = TIMESTAMPTZ '{batch_start.isoformat()}'")
        conn.execute(f"SET VARIABLE __batch_end = TIMESTAMPTZ '{batch_end.isoformat()}'")
        conn.execute("SET VARIABLE __window_hop = INTERVAL '5 minutes'")

        batch_result = conn.execute("""
            SELECT window_start, window_end, COUNT(*) as cnt
            FROM HOP('events', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start, window_end
            ORDER BY window_start
        """).fetchall()

        streaming_results = []
        window_start = batch_start
        window_end = batch_start + size
        while window_end <= batch_end:
            conn.execute(f"SET VARIABLE __batch_start = TIMESTAMPTZ '{window_start.isoformat()}'")
            conn.execute(f"SET VARIABLE __batch_end = TIMESTAMPTZ '{window_end.isoformat()}'")

            window_result = conn.execute("""
                SELECT window_start, window_end, COUNT(*) as cnt
                FROM HOP('events', 'event_time', INTERVAL '10 minutes')
                GROUP BY window_start, window_end
            """).fetchall()
            streaming_results.extend(window_result)
            window_start = window_start + hop
            window_end = window_end + hop

        streaming_results.sort(key=lambda x: x[0])

        assert batch_result == streaming_results
