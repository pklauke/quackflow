import asyncio
import datetime as dt

import pyarrow as pa

from quackflow.time_notion import TimeNotion


class FakeSource:
    def __init__(
        self,
        batches: list[pa.RecordBatch],
        time_notion: TimeNotion,
        delay_between_batches: float = 0.0,
    ):
        self._batches = batches
        self._time_notion = time_notion
        self._delay = delay_between_batches
        self._index = 0
        self._watermark: dt.datetime | None = None
        self._started = False
        self._stopped = False

    @property
    def watermark(self) -> dt.datetime | None:
        return self._watermark

    async def start(self) -> None:
        self._started = True

    async def seek(self, timestamp: dt.datetime) -> None:
        pass

    async def read(self) -> pa.RecordBatch:
        if self._index >= len(self._batches):
            # Signal exhaustion by advancing watermark to max
            self._watermark = dt.datetime.max.replace(tzinfo=dt.timezone.utc)
            schema = self._batches[0].schema if self._batches else None
            return pa.RecordBatch.from_pydict({col: [] for col in schema.names}, schema=schema)
        if self._delay > 0 and self._index > 0:
            await asyncio.sleep(self._delay)
        batch = self._batches[self._index]
        self._index += 1
        if batch.num_rows > 0:
            self._watermark = self._time_notion.compute_watermark(batch)
        return batch

    async def stop(self) -> None:
        self._stopped = True
