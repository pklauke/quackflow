import datetime as dt

import duckdb
import pyarrow as pa

from quackflow.schema import Schema
from quackflow.window import register_window_functions


class Engine:
    def __init__(self, database: str = ":memory:"):
        self._conn = duckdb.connect(database)
        self._conn.execute("SET VARIABLE __batch_start = TIMESTAMP '1970-01-01 00:00:00'")
        self._conn.execute("SET VARIABLE __batch_end = TIMESTAMP '1970-01-01 00:00:00'")
        self._conn.execute("SET VARIABLE __window_hop = INTERVAL '1 minute'")
        register_window_functions(self._conn)

    def create_table(self, name: str, schema: type[Schema]) -> None:
        ddl = schema.create_table_ddl(name)
        self._conn.execute(ddl)

    def create_view(self, name: str, sql: str) -> None:
        self._conn.execute(f"CREATE VIEW {name} AS {sql}")

    def insert(self, table_name: str, batch: pa.RecordBatch) -> None:
        self._conn.execute(f"INSERT INTO {table_name} SELECT * FROM batch")

    def set_batch_start(self, batch_start: dt.datetime) -> None:
        self._conn.execute("SET VARIABLE __batch_start = $1::TIMESTAMP", [batch_start])

    def set_batch_end(self, batch_end: dt.datetime) -> None:
        self._conn.execute("SET VARIABLE __batch_end = $1::TIMESTAMP", [batch_end])

    def set_window_hop(self, hop: dt.timedelta) -> None:
        self._conn.execute("SET VARIABLE __window_hop = $1::INTERVAL", [hop])

    def query(self, sql: str) -> pa.RecordBatch:
        result = self._conn.execute(sql).fetch_arrow_table()
        return pa.RecordBatch.from_pydict(
            {col: result.column(col).combine_chunks() for col in result.column_names},
            schema=result.schema,
        )

    def delete_before(self, table_name: str, ts_col: str, threshold: dt.datetime) -> int:
        result = self._conn.execute(f"DELETE FROM {table_name} WHERE {ts_col} < $1::TIMESTAMP", [threshold])
        row = result.fetchone()
        return row[0] if row else 0
