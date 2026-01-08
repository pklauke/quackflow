import typing

import pyarrow as pa


@typing.runtime_checkable
class Sink(typing.Protocol):
    async def write(self, batch: pa.RecordBatch) -> None: ...
