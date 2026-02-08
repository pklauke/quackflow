import datetime as dt
import logging
import threading

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)

from quackflow._internal.window import register_window_functions
from quackflow.schema import Schema


class EngineContext:
    """Thread-safe execution context for database operations.

    All operations are serialized with a lock because DuckDB cursors
    are not thread-safe. When a query runs in asyncio.to_thread(),
    other operations (inserts from receive_watermark) must wait.
    """

    def __init__(self, cursor: duckdb.DuckDBPyConnection):
        self._cursor = cursor
        self._lock = threading.Lock()

    def insert(self, table_name: str, batch: pa.RecordBatch) -> None:
        with self._lock:
            self._cursor.execute(f"INSERT INTO {table_name} SELECT * FROM batch")

    def insert_or_create(self, table_name: str, batch: pa.RecordBatch) -> None:
        """Insert into table, creating it first if it doesn't exist."""
        with self._lock:
            self._cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM batch WHERE false")
            self._cursor.execute(f"INSERT INTO {table_name} SELECT * FROM batch")

    def query(self, sql: str) -> pa.RecordBatch:
        with self._lock:
            try:
                result = self._cursor.execute(sql).fetch_arrow_table()
            except Exception as e:
                logger.error("DuckDB query failed: %s\nSQL: %s", e, sql[:500])
                raise
            return pa.RecordBatch.from_pydict(
                {col: result.column(col).combine_chunks() for col in result.column_names},
                schema=result.schema,
            )

    def query_with_window(
        self,
        sql: str,
        batch_start: dt.datetime,
        batch_end: dt.datetime,
        window_hop: dt.timedelta,
    ) -> pa.RecordBatch:
        """Execute query with window variables set atomically."""
        with self._lock:
            self._cursor.execute("SET VARIABLE __batch_start = $1::TIMESTAMPTZ", [batch_start])
            self._cursor.execute("SET VARIABLE __batch_end = $1::TIMESTAMPTZ", [batch_end])
            self._cursor.execute("SET VARIABLE __window_hop = $1::INTERVAL", [window_hop])
            try:
                result = self._cursor.execute(sql).fetch_arrow_table()
            except Exception as e:
                logger.error("DuckDB query failed: %s\nSQL: %s", e, sql[:500])
                raise
            return pa.RecordBatch.from_pydict(
                {col: result.column(col).combine_chunks() for col in result.column_names},
                schema=result.schema,
            )

    def delete_before(self, table_name: str, ts_col: str, threshold: dt.datetime) -> int:
        with self._lock:
            result = self._cursor.execute(f"DELETE FROM {table_name} WHERE {ts_col} < $1::TIMESTAMPTZ", [threshold])
            row = result.fetchone()
            return row[0] if row else 0


class Engine:
    def __init__(self, database: str = ":memory:"):
        self._conn = duckdb.connect(database)
        self._conn.execute("SET VARIABLE __batch_start = TIMESTAMPTZ '1970-01-01 00:00:00+00'")
        self._conn.execute("SET VARIABLE __batch_end = TIMESTAMPTZ '1970-01-01 00:00:00+00'")
        self._conn.execute("SET VARIABLE __window_hop = INTERVAL '1 minute'")
        register_window_functions(self._conn)

    def create_context(self) -> EngineContext:
        """Create an execution context using a cursor from the shared connection."""
        return EngineContext(self._conn.cursor())

    def create_table(self, name: str, schema: type[Schema]) -> None:
        ddl = schema.create_table_ddl(name)
        self._conn.execute(ddl)

    def create_view(self, name: str, sql: str) -> None:
        self._conn.execute(f"CREATE VIEW {name} AS {sql}")
