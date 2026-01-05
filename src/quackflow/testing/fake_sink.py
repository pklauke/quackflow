import pyarrow as pa


class FakeSink:
    def __init__(self):
        self.batches: list[pa.RecordBatch] = []

    async def write(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)

    def to_dicts(self) -> list[list[dict]]:
        return [
            [{col: batch.column(col)[i].as_py() for col in batch.schema.names} for i in range(batch.num_rows)]
            for batch in self.batches
        ]
