import datetime as dt

import pyarrow as pa
import pytest

from quackflow.source import ReplayableSource, Source
from quackflow.time_notion import EventTimeNotion, TimeNotion


class FakeSource:
    def __init__(self, batch: pa.RecordBatch, time_notion: TimeNotion):
        self._batch = batch
        self._time_notion = time_notion
        self._watermark: dt.datetime | None = None
        self._read_count = 0
        self._started = False
        self._stopped = False

    @property
    def watermark(self) -> dt.datetime | None:
        return self._watermark

    async def start(self) -> None:
        self._started = True

    async def read(self) -> pa.RecordBatch:
        if self._read_count > 0:
            return pa.RecordBatch.from_pydict({col: [] for col in self._batch.schema.names}, schema=self._batch.schema)
        self._read_count += 1
        self._update_watermark(self._batch)
        return self._batch

    async def stop(self) -> None:
        self._stopped = True

    def _update_watermark(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows == 0:
            return
        new_watermark = self._time_notion.compute_watermark(batch)
        if self._watermark is None or new_watermark > self._watermark:
            self._watermark = new_watermark


class FakeReplayableSource(FakeSource):
    def __init__(self, batch: pa.RecordBatch, time_notion: EventTimeNotion):
        super().__init__(batch, time_notion)
        self._seek_index = 0

    async def seek(self, timestamp: dt.datetime) -> None:
        if not isinstance(self._time_notion, EventTimeNotion):
            raise ValueError("seek requires EventTimeNotion")
        timestamps = self._batch.column(self._time_notion.column).to_pylist()
        for i, ts in enumerate(timestamps):
            if ts >= timestamp:
                self._seek_index = i
                return
        self._seek_index = len(timestamps)

    async def read(self) -> pa.RecordBatch:
        if self._read_count > 0:
            return pa.RecordBatch.from_pydict({col: [] for col in self._batch.schema.names}, schema=self._batch.schema)
        self._read_count += 1
        result = self._batch.slice(self._seek_index)
        self._update_watermark(result)
        return result


class TestSourceProtocol:
    def test_fake_source_implements_protocol(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1],
                "timestamp": [dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)],
            }
        )
        source = FakeSource(batch, EventTimeNotion(column="timestamp"))

        assert isinstance(source, Source)

    @pytest.mark.asyncio
    async def test_source_lifecycle(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "timestamp": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
                ],
            }
        )
        source = FakeSource(batch, EventTimeNotion(column="timestamp"))

        await source.start()
        result = await source.read()
        await source.stop()

        assert source._started is True
        assert source._stopped is True
        assert result.num_rows == 2

    @pytest.mark.asyncio
    async def test_source_read_empty_when_exhausted(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1],
                "timestamp": [dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)],
            }
        )
        source = FakeSource(batch, EventTimeNotion(column="timestamp"))

        await source.start()
        await source.read()
        empty = await source.read()

        assert empty.num_rows == 0

    @pytest.mark.asyncio
    async def test_watermark_updates_after_read(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "timestamp": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
                ],
            }
        )
        source = FakeSource(batch, EventTimeNotion(column="timestamp"))

        assert source.watermark is None
        await source.start()
        await source.read()

        assert source.watermark == dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)


class TestReplayableSourceProtocol:
    def test_fake_replayable_source_implements_protocol(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1],
                "timestamp": [dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)],
            }
        )
        source = FakeReplayableSource(batch, EventTimeNotion(column="timestamp"))

        assert isinstance(source, ReplayableSource)

    @pytest.mark.asyncio
    async def test_seek_to_timestamp(self):
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "timestamp": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
                ],
            }
        )
        source = FakeReplayableSource(batch, EventTimeNotion(column="timestamp"))

        await source.start()
        await source.seek(dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc))
        result = await source.read()

        assert result.to_pydict()["id"] == [2, 3]
