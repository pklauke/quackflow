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


class CountSchema(Schema):
    cnt = Int()
    window_end = Timestamp()


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
        # Use HOP to get window_end column for proper window-based batching
        app.output(
            "results",
            "SELECT id, user_id, event_time, window_end FROM HOP('events', 'event_time', INTERVAL '30 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=30))

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
        )

        # With unified processing, results are split by window_end
        assert len(sink.batches) == 2

    @pytest.mark.asyncio
    async def test_gradual_watermark_fires_windows_one_at_a_time(self):
        """When watermark advances gradually, each window fires separately."""
        time_notion = EventTimeNotion(column="event_time")
        # Three separate batches, each advancing watermark by 10 minutes
        # Last batch must reach end time (10:30) to terminate
        batch1 = make_batch([1], ["alice"], [dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc)])
        batch2 = make_batch([2], ["bob"], [dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc)])
        batch3 = make_batch([3], ["charlie"], [dt.datetime(2024, 1, 1, 10, 25, tzinfo=dt.timezone.utc)])
        # Final batch to push watermark past end time
        batch4 = make_batch([4], ["david"], [dt.datetime(2024, 1, 1, 10, 35, tzinfo=dt.timezone.utc)])
        source = FakeSource([batch1, batch2, batch3, batch4], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output(
            "results",
            "SELECT COUNT(*) as cnt, window_end FROM HOP('events', 'event_time', INTERVAL '10 minutes') GROUP BY window_end",
            schema=CountSchema,
        ).trigger(window=dt.timedelta(minutes=10))

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc),
        )

        # batch1 (10:05) -> no fire (snap 10:00 == last_fired 10:00)
        # batch2 (10:15) -> fires window 10:10 (snap 10:10 > 10:00)
        # batch3 (10:25) -> fires window 10:20 (snap 10:20 > 10:10)
        # batch4 (10:35) -> watermark >= end, stops. Final fire for window 10:30
        assert len(sink.batches) == 3
        counts = [b.to_pydict()["cnt"][0] for b in sink.batches]
        assert counts == [1, 1, 1]  # One event per window

    @pytest.mark.asyncio
    async def test_watermark_jump_fires_multiple_windows_at_once(self):
        """When watermark jumps, all intermediate windows fire in one query."""
        time_notion = EventTimeNotion(column="event_time")
        # Single batch with events spanning 30 minutes - watermark jumps past end
        batch = make_batch(
            [1, 2, 3, 4],
            ["alice", "bob", "charlie", "david"],
            [
                dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 25, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 35, tzinfo=dt.timezone.utc),  # Past end time
            ],
        )
        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output(
            "results",
            "SELECT COUNT(*) as cnt, window_end FROM HOP('events', 'event_time', INTERVAL '10 minutes') GROUP BY window_end",
            schema=CountSchema,
        ).trigger(window=dt.timedelta(minutes=10))

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink})
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 30, tzinfo=dt.timezone.utc),
        )

        # Watermark jumps to 10:35, triggering windows 10:10, 10:20, 10:30 in one query
        # Results are still split by window_end for ordered emission
        assert len(sink.batches) == 3
        window_ends = [b.to_pydict()["window_end"][0] for b in sink.batches]
        assert window_ends[0] < window_ends[1] < window_ends[2]  # Ordered by window_end


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
    async def test_view_joining_source_and_view(self):
        """View depending on both a source and another view must receive watermarks from both."""
        time_notion = EventTimeNotion(column="event_time")

        # Source A: goes through a view first
        a_batch = make_batch(
            [1, 2],
            ["alice", "bob"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
            ],
        )
        source_a = FakeSource([a_batch], time_notion)

        # Source B: direct to join
        b_batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "user_id": ["alice", "bob"],
                "event_time": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
                ],
            }
        )
        source_b = FakeSource([b_batch], time_notion)

        sink = FakeSink()

        app = Quackflow()
        app.source("source_a", schema=EventSchema, ts_col="event_time")
        app.source("source_b", schema=EventSchema, ts_col="event_time")
        app.view("filtered_a", "SELECT * FROM source_a WHERE user_id != 'nobody'")
        app.view(
            "joined",
            """SELECT a.id, a.user_id, a.event_time
               FROM HOP('filtered_a', 'event_time', INTERVAL '5 minutes') a
               JOIN HOP('source_b', 'event_time', INTERVAL '5 minutes') b
               ON a.id = b.id AND a.window_end = b.window_end""",
        )
        app.output(
            "results",
            "SELECT * FROM HOP('joined', 'event_time', INTERVAL '5 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        runtime = Runtime(
            app,
            sources={"source_a": source_a, "source_b": source_b},
            sinks={"results": sink},
        )
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
        )

        total_rows = sum(b.num_rows for b in sink.batches)
        assert total_rows > 0, "Output should have received data from join"

    @pytest.mark.asyncio
    async def test_expiration_flows_through_join_to_both_sources(self):
        """Expiration must reach both sources through a view with multiple upstreams."""
        time_notion = EventTimeNotion(column="event_time")

        events_batch1 = make_batch(
            [1, 2],
            ["alice", "bob"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 1, tzinfo=dt.timezone.utc),
            ],
        )
        events_batch2 = make_batch(
            [3],
            ["charlie"],
            [dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc)],
        )
        events_source = FakeSource([events_batch1, events_batch2], time_notion)

        meta_batch1 = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "user_id": ["alice", "bob"],
                "event_time": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 1, tzinfo=dt.timezone.utc),
                ],
            }
        )
        meta_batch2 = pa.RecordBatch.from_pydict(
            {
                "id": [3],
                "user_id": ["charlie"],
                "event_time": [dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc)],
            }
        )
        meta_source = FakeSource([meta_batch1, meta_batch2], time_notion)

        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema, ts_col="event_time")
        app.source("metadata", schema=EventSchema, ts_col="event_time")
        app.view(
            "joined",
            """SELECT e.id, e.user_id, e.event_time
               FROM HOP('events', 'event_time', INTERVAL '5 minutes') e
               JOIN HOP('metadata', 'event_time', INTERVAL '5 minutes') m
               ON e.id = m.id AND e.window_end = m.window_end""",
        )
        app.output(
            "results",
            "SELECT * FROM HOP('joined', 'event_time', INTERVAL '5 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        runtime = Runtime(
            app,
            sources={"events": events_source, "metadata": meta_source},
            sinks={"results": sink},
        )
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc),
        )

        events_count = runtime._engine.query("SELECT COUNT(*) as cnt FROM events").to_pydict()["cnt"][0]
        meta_count = runtime._engine.query("SELECT COUNT(*) as cnt FROM metadata").to_pydict()["cnt"][0]
        assert events_count == 1, f"events should have 1 row, got {events_count}"
        assert meta_count == 1, f"metadata should have 1 row, got {meta_count}"

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
        app.source("events", schema=EventSchema, ts_col="event_time")
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
        app.source("events", schema=EventSchema, ts_col="event_time")
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
        app.source("events", schema=EventSchema, ts_col="event_time")
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


class TestMultiplePartitions:
    @pytest.mark.asyncio
    async def test_two_partitions_same_result_as_one(self):
        """Running with 2 partitions should produce same results as 1 partition."""
        time_notion = EventTimeNotion(column="event_time")
        batch = make_batch(
            [1, 2, 3, 4, 5],
            ["alice", "bob", "alice", "bob", "bob"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 2, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 3, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 6, tzinfo=dt.timezone.utc),
            ],
        )

        # Run with 1 partition
        source1 = FakeSource([batch], time_notion)
        sink1 = FakeSink()

        app1 = Quackflow()
        app1.source("events", schema=EventSchema, ts_col="event_time")
        app1.output(
            "results",
            "SELECT COUNT(*) as cnt, window_end FROM HOP('events', 'event_time', INTERVAL '5 minutes') GROUP BY window_end",
            schema=CountSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        runtime1 = Runtime(app1, sources={"events": source1}, sinks={"results": sink1}, num_partitions=1)
        await runtime1.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
        )

        # Run with 2 partitions
        source2 = FakeSource([batch], time_notion)
        sink2 = FakeSink()

        app2 = Quackflow()
        app2.source("events", schema=EventSchema, ts_col="event_time")
        app2.output(
            "results",
            "SELECT COUNT(*) as cnt, window_end FROM HOP('events', 'event_time', INTERVAL '5 minutes') GROUP BY window_end",
            schema=CountSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        runtime2 = Runtime(app2, sources={"events": source2}, sinks={"results": sink2}, num_partitions=2)
        await runtime2.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 5, tzinfo=dt.timezone.utc),
        )

        # Both should produce the same count
        result1 = sink1.to_dicts()
        result2 = sink2.to_dicts()

        total1 = sum(row["cnt"] for batch in result1 for row in batch)
        total2 = sum(row["cnt"] for batch in result2 for row in batch)

        assert total1 == 4, f"1 partition should count 4, got {total1}"
        assert total2 == total1, f"2 partitions should match 1 partition: {total2} vs {total1}"

    @pytest.mark.asyncio
    async def test_partitions_dont_duplicate_output(self):
        """With shared engine, multiple output partitions shouldn't duplicate results."""
        time_notion = EventTimeNotion(column="event_time")
        batch = make_batch(
            [1, 2, 3, 4, 5],
            ["alice", "bob", "alice", "bob", "bob"],
            [
                dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 2, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 3, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 6, tzinfo=dt.timezone.utc),
            ],
        )

        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("events", schema=EventSchema, ts_col="event_time")
        app.output(
            "results",
            "SELECT * FROM events",
            schema=EventSchema,
        ).trigger(records=1)

        runtime = Runtime(app, sources={"events": source}, sinks={"results": sink}, num_partitions=2)
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 10, tzinfo=dt.timezone.utc),
        )

        # Should NOT have duplicates - each row should appear once
        total_rows = sum(b.num_rows for b in sink.batches)
        assert total_rows == 5, f"Expected 5 rows (no duplicates), got {total_rows}"
