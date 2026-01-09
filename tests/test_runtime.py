import datetime as dt

import pyarrow as pa
import pytest

from quackflow.app import Quackflow
from quackflow.batch_runtime import BatchRuntime
from quackflow.runtime import Runtime
from quackflow.schema import Int, Schema, String, Timestamp
from quackflow.testing import FakeSink, FakeSource
from quackflow.time_notion import EventTimeNotion


class EventSchema(Schema):
    id = Int()
    user_id = String()
    event_time = Timestamp()


def make_batch(ids: list[int], users: list[str], times: list[dt.datetime]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"id": ids, "user_id": users, "event_time": times})


class TestCompileValidation:
    def test_output_without_trigger_raises(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("results", "SELECT * FROM events", schema=EventSchema)

        with pytest.raises(ValueError, match="trigger"):
            app.compile()


@pytest.mark.parametrize("runtime_class", [Runtime, BatchRuntime])
class TestRuntimeBasic:
    @pytest.mark.asyncio
    async def test_source_to_output(self, runtime_class):
        time_notion = EventTimeNotion(column="event_time")
        batch = make_batch(
            [1, 2],
            ["alice", "bob"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
            ],
        )
        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(records=2)

        runtime = runtime_class(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        total_rows = sum(b.num_rows for b in sink.batches)
        assert total_rows == 2

    @pytest.mark.asyncio
    async def test_source_through_view_to_output(self, runtime_class):
        time_notion = EventTimeNotion(column="event_time")
        batch = make_batch(
            [1, 2, 3],
            ["alice", "bob", "alice"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
            ],
        )
        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view("alice_events", "SELECT * FROM events WHERE user_id = 'alice'")
        app.output("results", "SELECT * FROM alice_events", schema=EventSchema).trigger(records=1)

        runtime = runtime_class(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        total_rows = sum(b.num_rows for b in sink.batches)
        assert total_rows == 2


class TestRuntimeTriggers:
    @pytest.mark.asyncio
    async def test_records_trigger(self):
        time_notion = EventTimeNotion(column="event_time")
        batch1 = make_batch([1], ["alice"], [dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)])
        batch2 = make_batch([2], ["bob"], [dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc)])
        source = FakeSource([batch1, batch2], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(records=2)

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        assert len(sink.batches) == 1
        assert sink.batches[0].num_rows == 2

    @pytest.mark.asyncio
    async def test_window_trigger(self):
        time_notion = EventTimeNotion(column="event_time")
        batch = make_batch(
            [1, 2, 3],
            ["alice", "bob", "charlie"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
            ],
        )
        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(window=dt.timedelta(minutes=30))

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        assert len(sink.batches) == 2


class TestRuntimeMultipleOutputs:
    @pytest.mark.asyncio
    async def test_multiple_outputs_independent_triggers(self):
        time_notion = EventTimeNotion(column="event_time")
        batch1 = make_batch([1], ["alice"], [dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)])
        batch2 = make_batch([2], ["bob"], [dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc)])
        source = FakeSource([batch1, batch2], time_notion)
        sink1 = FakeSink()
        sink2 = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("output1", "SELECT * FROM events", schema=EventSchema).trigger(records=1)
        app.output("output2", "SELECT * FROM events", schema=EventSchema).trigger(records=2)

        runtime = Runtime(app, sources={"events": source}, sinks={"output1": sink1, "output2": sink2})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        assert len(sink1.batches) == 2
        assert len(sink2.batches) == 1


class TestDataExpiration:
    @pytest.mark.asyncio
    async def test_data_expires_after_window_fires(self):
        time_notion = EventTimeNotion(column="event_time")
        batch1 = make_batch(
            [1, 2],
            ["alice", "bob"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 1, tzinfo=dt.timezone.utc),
            ],
        )
        batch2 = make_batch(
            [3],
            ["charlie"],
            [dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc)],
        )
        source = FakeSource([batch1, batch2], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output(
            "results",
            "SELECT * FROM HOP('events', 'event_time', INTERVAL '5 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc),
        )

        final_count = runtime._engine.query("SELECT COUNT(*) as cnt FROM events").to_pydict()["cnt"][0]
        assert final_count == 1

    @pytest.mark.asyncio
    async def test_expiration_uses_minimum_threshold_from_multiple_outputs(self):
        time_notion = EventTimeNotion(column="event_time")
        batch = make_batch(
            [1, 2, 3],
            ["alice", "bob", "charlie"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc),
            ],
        )
        source = FakeSource([batch], time_notion)
        sink1 = FakeSink()
        sink2 = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output(
            "output_5min",
            "SELECT * FROM HOP('events', 'event_time', INTERVAL '5 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=5))
        app.output(
            "output_10min",
            "SELECT * FROM HOP('events', 'event_time', INTERVAL '10 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        runtime = Runtime(
            app,
            sources={"events": source},
            sinks={"output_5min": sink1, "output_10min": sink2},
        )
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc),
        )

        final_count = runtime._engine.query("SELECT COUNT(*) as cnt FROM events").to_pydict()["cnt"][0]
        assert final_count >= 2

    @pytest.mark.asyncio
    async def test_view_with_larger_window_prevents_early_expiration(self):
        time_notion = EventTimeNotion(column="event_time")
        batch = make_batch(
            [1, 2, 3],
            ["alice", "bob", "charlie"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc),
            ],
        )
        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view(
            "windowed_events",
            "SELECT * FROM HOP('events', 'event_time', INTERVAL '10 minutes')",
        )
        app.output(
            "results",
            "SELECT * FROM HOP('windowed_events', 'event_time', INTERVAL '5 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc),
        )

        final_count = runtime._engine.query("SELECT COUNT(*) as cnt FROM events").to_pydict()["cnt"][0]
        assert final_count >= 2
