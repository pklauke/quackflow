"""Tests for distributed execution mode."""

import datetime as dt

import pyarrow as pa
import pytest

from quackflow.app import Quackflow
from quackflow.repartition import repartition
from quackflow.schema import Int, Schema, String, Timestamp
from quackflow.testing import FakeSink, FakeSource
from quackflow.time_notion import EventTimeNotion

from .launcher import launch_distributed_cluster


class EventSchema(Schema):
    user_id = String()
    value = Int()
    event_time = Timestamp()


class AggSchema(Schema):
    window_end = Timestamp()
    user_id = String()
    total = Int()


def make_batch(
    users: list[str],
    values: list[int],
    times: list[dt.datetime],
) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {
            "user_id": users,
            "value": values,
            "event_time": times,
        }
    )


class TestDistributedWatermarkPropagation:
    @pytest.mark.asyncio
    async def test_watermarks_propagate_across_workers(self):
        """Test that watermarks correctly propagate between workers."""
        time_notion = EventTimeNotion(column="event_time")

        # Create test data with users that hash to different partitions
        # alice -> partition 0, charlie -> partition 1
        batch = make_batch(
            users=["alice", "charlie", "alice", "charlie"],
            values=[10, 20, 30, 40],
            times=[
                dt.datetime(2024, 1, 1, 10, 0, 10, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 0, 20, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 0, 40, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 1, 10, tzinfo=dt.timezone.utc),
            ],
        )

        # Partition data by user_id
        partitioned = repartition(batch, ["user_id"], num_partitions=2)

        # Create sources/sinks for each partition
        sources_by_partition = {}
        sinks_by_partition = {}
        for partition_id, partition_batch in partitioned.items():
            sources_by_partition[partition_id] = {"events": FakeSource([partition_batch], time_notion)}
            sinks_by_partition[partition_id] = {"results": FakeSink()}

        # Build app - simple output without windowing
        app = Quackflow()
        app.source("events", schema=EventSchema, partition_by=["user_id"])
        app.output(
            "results",
            "SELECT user_id, SUM(value) AS total FROM events GROUP BY user_id",
            schema=AggSchema,
            partition_by=["user_id"],
        ).trigger(records=1)

        # Run distributed cluster
        start = dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
        end = dt.datetime(2024, 1, 1, 10, 2, tzinfo=dt.timezone.utc)

        cluster = launch_distributed_cluster(
            app=app,
            sources_by_partition=sources_by_partition,
            sinks_by_partition=sinks_by_partition,
            num_workers=2,
            start=start,
            end=end,
            debug=True,
        )

        try:
            cluster.wait()
            batches = cluster.collect_sink_results("results")

            # Verify we got results from both workers
            total_rows = sum(b.num_rows for b in batches)
            assert total_rows >= 2, f"Expected at least 2 rows, got {total_rows}"

            # Verify correct aggregation per user
            all_rows = []
            for batch in batches:
                for i in range(batch.num_rows):
                    all_rows.append(
                        {
                            "user_id": batch.column("user_id")[i].as_py(),
                            "total": int(batch.column("total")[i].as_py()),
                        }
                    )

            alice_total = sum(r["total"] for r in all_rows if r["user_id"] == "alice")
            charlie_total = sum(r["total"] for r in all_rows if r["user_id"] == "charlie")

            assert alice_total == 40, f"Expected alice total 40, got {alice_total}"
            assert charlie_total == 60, f"Expected charlie total 60, got {charlie_total}"

        finally:
            cluster.shutdown()


class TestDistributedSimple:
    @pytest.mark.asyncio
    async def test_two_workers_basic(self):
        """Basic test with 2 workers, each processing their partition."""
        time_notion = EventTimeNotion(column="event_time")

        # Create simple data for each partition
        batch0 = make_batch(
            users=["alice"],
            values=[100],
            times=[dt.datetime(2024, 1, 1, 10, 0, 30, tzinfo=dt.timezone.utc)],
        )
        batch1 = make_batch(
            users=["bob"],
            values=[200],
            times=[dt.datetime(2024, 1, 1, 10, 0, 30, tzinfo=dt.timezone.utc)],
        )

        sources_by_partition = {
            0: {"events": FakeSource([batch0], time_notion)},
            1: {"events": FakeSource([batch1], time_notion)},
        }
        sinks_by_partition = {
            0: {"results": FakeSink()},
            1: {"results": FakeSink()},
        }

        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output(
            "results",
            "SELECT user_id, SUM(value) AS total FROM events GROUP BY user_id",
            schema=AggSchema,
        ).trigger(records=1)

        start = dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
        end = dt.datetime(2024, 1, 1, 10, 1, tzinfo=dt.timezone.utc)

        cluster = launch_distributed_cluster(
            app=app,
            sources_by_partition=sources_by_partition,
            sinks_by_partition=sinks_by_partition,
            num_workers=2,
            start=start,
            end=end,
            debug=True,
        )

        try:
            cluster.wait()
            batches = cluster.collect_sink_results("results")

            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 2, f"Expected 2 rows (one per user), got {total_rows}"

        finally:
            cluster.shutdown()
