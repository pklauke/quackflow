import duckdb


def register_window_functions_streaming(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE OR REPLACE FUNCTION HOP(tbl, ts_col, size) AS TABLE
        SELECT
            t.*,
            getvariable('__window_end')::TIMESTAMP - size AS window_start,
            getvariable('__window_end')::TIMESTAMP AS window_end
        FROM query_table(tbl) t
        WHERE t[ts_col] >= getvariable('__window_end')::TIMESTAMP - size
          AND t[ts_col] < getvariable('__window_end')::TIMESTAMP
    """)


def register_window_functions_batch(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE OR REPLACE FUNCTION HOP(tbl, ts_col, size) AS TABLE
        WITH __windows AS (
            SELECT
                ws AS window_start,
                ws + size AS window_end
            FROM generate_series(
                getvariable('__batch_start')::TIMESTAMP,
                getvariable('__batch_end')::TIMESTAMP - size,
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
