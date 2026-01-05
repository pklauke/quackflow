import datetime as dt

import duckdb
import pyarrow as pa

from quackflow.schema import Schema
from quackflow.window import register_window_functions_streaming


class Engine:
    def __init__(self, database: str = ":memory:"):
        self._conn = duckdb.connect(database)
        register_window_functions_streaming(self._conn)

    def create_table(self, name: str, schema: type[Schema]) -> None:
        ddl = schema.create_table_ddl(name)
        self._conn.execute(ddl)

    def create_view(self, name: str, sql: str) -> None:
        self._conn.execute(f"CREATE VIEW {name} AS {sql}")

    def insert(self, table_name: str, batch: pa.RecordBatch) -> None:
        self._conn.execute(f"INSERT INTO {table_name} SELECT * FROM batch")

    def set_window_end(self, window_end: dt.datetime) -> None:
        self._conn.execute("SET VARIABLE __window_end = $1::TIMESTAMP", [window_end])

    def query(self, sql: str) -> pa.RecordBatch:
        result = self._conn.execute(sql).fetch_arrow_table()
        return pa.RecordBatch.from_pydict(
            {col: result.column(col).combine_chunks() for col in result.column_names},
            schema=result.schema,
        )
