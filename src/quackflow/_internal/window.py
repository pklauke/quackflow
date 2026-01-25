import duckdb


def register_window_functions(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE OR REPLACE FUNCTION HOP(tbl, ts_col, size) AS TABLE
        WITH __windows AS (
            SELECT
                ws AS window_start,
                ws + size AS window_end
            FROM generate_series(
                getvariable('__batch_start')::TIMESTAMPTZ,
                getvariable('__batch_end')::TIMESTAMPTZ - size,
                getvariable('__window_hop')::INTERVAL
            ) AS t(ws)
        )
        SELECT
            t.*,
            w.window_start,
            w.window_end
        FROM query_table(tbl) t
        JOIN __windows w
            ON t[ts_col] >= w.window_start
            AND t[ts_col] < w.window_end
    """)
