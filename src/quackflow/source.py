import datetime as dt
import typing

import pyarrow as pa


@typing.runtime_checkable
class Source(typing.Protocol):
    @property
    def watermark(self) -> dt.datetime | None: ...

    async def start(self) -> None: ...

    async def read(self) -> pa.RecordBatch: ...

    async def stop(self) -> None: ...


@typing.runtime_checkable
class ReplayableSource(Source, typing.Protocol):
    async def seek(self, timestamp: dt.datetime) -> None: ...
