import duckdb
import pyarrow as pa

from quackflow.schema import Schema


class Engine:
    def __init__(self, database: str = ":memory:"):
        self._conn = duckdb.connect(database)

    def create_table(self, name: str, schema: type[Schema]) -> None:
        ddl = schema.create_table_ddl(name)
        self._conn.execute(ddl)

    def insert(self, table_name: str, batch: pa.RecordBatch) -> None:
        self._conn.execute(f"INSERT INTO {table_name} SELECT * FROM batch")

    def query(self, sql: str) -> pa.RecordBatch:
        result = self._conn.execute(sql).fetch_arrow_table()
        return pa.RecordBatch.from_pydict(
            {col: result.column(col).combine_chunks() for col in result.column_names},
            schema=result.schema,
        )
