import typing

import pyarrow as pa


@typing.runtime_checkable
class Sink(typing.Protocol):
    async def start(self) -> None: ...
    async def write(self, batch: pa.RecordBatch) -> None: ...
    async def stop(self) -> None: ...
