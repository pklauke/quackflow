import datetime as dt

import pyarrow as pa
import pytest

from quackflow.app import Quackflow
from quackflow.runtime import Runtime
from quackflow.schema import Int, Schema, String, Timestamp
from quackflow.time_notion import EventTimeNotion


class EventSchema(Schema):
    id = Int()
    user_id = String()
    event_time = Timestamp()


class FakeSource:
    def __init__(self, batches: list[pa.RecordBatch]):
        self._batches = batches
        self._index = 0
        self._watermark: dt.datetime | None = None
        self._time_notion: EventTimeNotion | None = None
        self._started = False

    @property
    def watermark(self) -> dt.datetime | None:
        return self._watermark

    def set_time_notion(self, time_notion: EventTimeNotion) -> None:
        self._time_notion = time_notion

    async def start(self) -> None:
        self._started = True

    async def seek(self, timestamp: dt.datetime) -> None:
        pass

    async def read(self) -> pa.RecordBatch:
        if self._index >= len(self._batches):
            schema = self._batches[0].schema if self._batches else None
            return pa.RecordBatch.from_pydict({"id": [], "user_id": [], "event_time": []}, schema=schema)
        batch = self._batches[self._index]
        self._index += 1
        if self._time_notion and batch.num_rows > 0:
            self._watermark = self._time_notion.compute_watermark(batch)
        return batch

    async def stop(self) -> None:
        pass


class FakeSink:
    def __init__(self):
        self.batches: list[pa.RecordBatch] = []

    async def write(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)


def make_batch(ids: list[int], users: list[str], times: list[dt.datetime]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"id": ids, "user_id": users, "event_time": times})


class TestCompileValidation:
    def test_output_without_trigger_raises(self):
        source = FakeSource([])
        sink = FakeSink()

        app = Quackflow()
        app.source("events", source, schema=EventSchema, time_notion=EventTimeNotion(column="event_time"))
        app.output(sink, "SELECT * FROM events", schema=EventSchema, depends_on=["events"])

        with pytest.raises(ValueError, match="trigger"):
            app.compile()


class TestRuntimeBasic:
    @pytest.mark.asyncio
    async def test_source_to_output(self):
        batch = make_batch(
            [1, 2],
            ["alice", "bob"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),  # reaches end
            ],
        )
        source = FakeSource([batch])
        sink = FakeSink()

        app = Quackflow()
        app.source("events", source, schema=EventSchema, time_notion=EventTimeNotion(column="event_time"))
        app.output(sink, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(records=2)

        runtime = Runtime(app)
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        assert len(sink.batches) == 1
        assert sink.batches[0].num_rows == 2

    @pytest.mark.asyncio
    async def test_source_through_view_to_output(self):
        batch = make_batch(
            [1, 2, 3],
            ["alice", "bob", "alice"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),  # reaches end
            ],
        )
        source = FakeSource([batch])
        sink = FakeSink()

        app = Quackflow()
        app.source("events", source, schema=EventSchema, time_notion=EventTimeNotion(column="event_time"))
        app.view("alice_events", "SELECT * FROM events WHERE user_id = 'alice'", depends_on=["events"])
        app.output(sink, "SELECT * FROM alice_events", schema=EventSchema, depends_on=["alice_events"]).trigger(
            records=1
        )

        runtime = Runtime(app)
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        assert len(sink.batches) >= 1
        total_rows = sum(b.num_rows for b in sink.batches)
        assert total_rows == 2  # only alice's events


class TestRuntimeTriggers:
    @pytest.mark.asyncio
    async def test_records_trigger(self):
        batch1 = make_batch([1], ["alice"], [dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)])
        batch2 = make_batch([2], ["bob"], [dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc)])  # reaches end
        source = FakeSource([batch1, batch2])
        sink = FakeSink()

        app = Quackflow()
        app.source("events", source, schema=EventSchema, time_notion=EventTimeNotion(column="event_time"))
        app.output(sink, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(records=2)

        runtime = Runtime(app)
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        assert len(sink.batches) == 1
        assert sink.batches[0].num_rows == 2


class TestRuntimeMultipleOutputs:
    @pytest.mark.asyncio
    async def test_multiple_outputs_independent_triggers(self):
        batch1 = make_batch([1], ["alice"], [dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)])
        batch2 = make_batch([2], ["bob"], [dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc)])  # reaches end
        source = FakeSource([batch1, batch2])
        sink1 = FakeSink()
        sink2 = FakeSink()

        app = Quackflow()
        app.source("events", source, schema=EventSchema, time_notion=EventTimeNotion(column="event_time"))
        app.output(sink1, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(records=1)
        app.output(sink2, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(records=2)

        runtime = Runtime(app)
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        assert len(sink1.batches) == 2  # fires after each record
        assert len(sink2.batches) == 1  # fires once after 2 records
