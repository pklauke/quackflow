"""Fixtures for distributed tests."""

import datetime as dt

import pyarrow as pa
import pytest

from quackflow.repartition import repartition
from quackflow.schema import Int, Schema, String, Timestamp
from quackflow.testing import FakeSink, FakeSource
from quackflow.time_notion import EventTimeNotion

from .launcher import DistributedCluster, launch_distributed_cluster


class GameSchema(Schema):
    name = String()
    team = String()
    score = Int()
    event_time = Timestamp()


class ScoreSchema(Schema):
    name = String()
    total = Int()


class TeamScoreSchema(Schema):
    team = String()
    total = Int()


# Base date for timestamps
BASE = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


def ts(minutes: int, seconds: int) -> dt.datetime:
    """Create timestamp relative to BASE (12:00:00)."""
    return BASE + dt.timedelta(minutes=minutes, seconds=seconds)


@pytest.fixture
def time_notion():
    return EventTimeNotion(column="event_time")


@pytest.fixture
def team_red_data():
    """Team red dataset. Names hash to different partitions:
    Partition 0: alice, bob, dave, eve
    Partition 1: charlie, leo, nick
    """
    return pa.RecordBatch.from_pydict({
        "name": ["alice", "bob", "leo", "dave", "alice", "nick", "charlie", "eve", "nick"],
        "team": ["red"] * 9,
        "score": [5, 7, 3, 4, 8, 3, 9, 8, 1],
        "event_time": [
            ts(0, 23),   # alice: 12:00:23
            ts(2, 26),   # bob:   12:02:26
            ts(3, 30),   # leo:   12:03:30
            ts(4, 17),   # dave:  12:04:17
            ts(3, 5),    # alice: 12:03:05
            ts(6, 38),   # nick:  12:06:38
            ts(1, 27),   # charlie: 12:01:27
            ts(7, 25),   # eve:   12:07:25
            ts(7, 47),   # nick:  12:07:47
        ],
    })


@pytest.fixture
def run_cluster(time_notion):
    """Fixture that provides a helper to run a distributed cluster."""
    clusters: list[DistributedCluster] = []

    def _run(
        app,
        batches_by_partition: dict[int, list[pa.RecordBatch]],
        num_workers: int = 2,
        start: dt.datetime = BASE,
        end: dt.datetime = BASE + dt.timedelta(minutes=10),
        debug: bool = True,
    ) -> list[pa.RecordBatch]:
        """Run cluster and return collected sink results."""
        sources = {
            pid: {"scores": FakeSource(batches, time_notion)}
            for pid, batches in batches_by_partition.items()
        }
        sinks = {pid: {"results": FakeSink()} for pid in batches_by_partition}

        cluster = launch_distributed_cluster(
            app=app,
            sources_by_partition=sources,
            sinks_by_partition=sinks,
            num_workers=num_workers,
            start=start,
            end=end,
            debug=debug,
        )
        clusters.append(cluster)

        cluster.wait()
        return cluster.collect_sink_results("results")

    yield _run

    for cluster in clusters:
        cluster.shutdown()


def partition_batch(batch: pa.RecordBatch, key: str = "name", num_partitions: int = 2) -> dict[int, list[pa.RecordBatch]]:
    """Partition a batch and wrap each in a list."""
    partitioned = repartition(batch, [key], num_partitions)
    return {pid: [b] for pid, b in partitioned.items()}


def collect_totals(batches: list[pa.RecordBatch], name_col: str = "name", total_col: str = "total") -> dict[str, int]:
    """Extract name -> total mapping from result batches."""
    totals: dict[str, int] = {}
    for batch in batches:
        for i in range(batch.num_rows):
            name = batch.column(name_col)[i].as_py()
            total = int(batch.column(total_col)[i].as_py())
            totals[name] = totals.get(name, 0) + total
    return totals
