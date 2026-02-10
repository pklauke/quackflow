"""Tests for distributed execution mode."""

import datetime as dt

import pytest

from quackflow.app import Quackflow

pytestmark = pytest.mark.timeout(15)

from .conftest import (
    BASE,
    GameSchema,
    ScoreSchema,
    TeamScoreSchema,
    WindowedCountSchema,
    collect_totals,
    partition_batch,
    ts,
)


class TestDistributedBasic:
    @pytest.mark.asyncio
    async def test_two_workers_process_partitioned_data(self, run_cluster, team_red_data):
        """Each worker processes its partition independently."""
        app = Quackflow()
        app.source("scores", schema=GameSchema, partition_by=["name"])
        app.output(
            "results",
            "SELECT name, SUM(score) AS total FROM scores GROUP BY name",
            schema=ScoreSchema,
            partition_by=["name"],
        ).trigger(records=1)

        results = run_cluster(app, partition_batch(team_red_data))
        totals = collect_totals(results)

        # alice: 5 + 8 = 13, bob: 7, charlie: 9, dave: 4, eve: 8, leo: 3, nick: 3 + 1 = 4
        assert totals["alice"] == 13
        assert totals["bob"] == 7
        assert totals["charlie"] == 9
        assert totals["nick"] == 4

    @pytest.mark.asyncio
    async def test_repartition_by_team(self, run_cluster, team_red_data):
        """Data is repartitioned by team before aggregation."""
        app = Quackflow()
        app.source("scores", schema=GameSchema, partition_by=["name"])
        app.output(
            "results",
            "SELECT team, SUM(score) AS total FROM scores GROUP BY team",
            schema=TeamScoreSchema,
            partition_by=["team"],
        ).trigger(records=1)

        results = run_cluster(app, partition_batch(team_red_data))

        # "red" hashes to partition 0, so only ONE output should produce results
        # with the complete sum of 48 (not two outputs with partial sums)
        assert len(results) == 1, f"Expected 1 result batch, got {len(results)}"
        assert results[0].column("team")[0].as_py() == "red"
        assert results[0].column("total")[0].as_py() == 48


class TestDistributedWindowTriggers:
    @pytest.mark.asyncio
    async def test_window_trigger_produces_correct_windows(self, run_cluster, team_red_data):
        """Window trigger fires at correct boundaries with correct counts."""
        app = Quackflow()
        app.source("scores", schema=GameSchema, partition_by=["name"])
        app.output(
            "results",
            """
            SELECT COUNT(*) as cnt, window_end
            FROM HOP('scores', 'event_time', INTERVAL '2 minutes')
            GROUP BY window_end
            """,
            schema=WindowedCountSchema,
        ).trigger(window=dt.timedelta(minutes=2))

        # Data spans 12:00:23 to 12:07:47, with 2-minute windows
        # Watermark advances to 12:07:47, which snaps to 12:06
        # So we can only emit windows up to 12:06 (window 12:08 isn't closed yet)
        #
        # Window 12:02: events from 12:00:23, 12:01:27 = 2 events
        # Window 12:04: events from 12:02:26, 12:03:05, 12:03:30 = 3 events
        # Window 12:06: events from 12:04:17 = 1 event
        results = run_cluster(
            app,
            partition_batch(team_red_data),
            start=BASE,
            end=BASE + dt.timedelta(minutes=10),
        )

        # Collect all window_end -> count
        windows = {}
        for batch in results:
            for i in range(batch.num_rows):
                window_end = batch.column("window_end")[i].as_py()
                cnt = batch.column("cnt")[i].as_py()
                windows[window_end] = windows.get(window_end, 0) + cnt

        assert windows[ts(2, 0)] == 2  # 12:02:00
        assert windows[ts(4, 0)] == 3  # 12:04:00
        assert windows[ts(6, 0)] == 1  # 12:06:00
        assert len(windows) == 3  # Only 3 windows emitted

    @pytest.mark.asyncio
    async def test_first_window_at_start_time(self, run_cluster, team_red_data):
        """First emitted window_end equals start time when data exists."""
        app = Quackflow()
        app.source("scores", schema=GameSchema, partition_by=["name"])
        app.output(
            "results",
            """
            SELECT COUNT(*) as cnt, window_end
            FROM HOP('scores', 'event_time', INTERVAL '2 minutes')
            GROUP BY window_end
            """,
            schema=WindowedCountSchema,
        ).trigger(window=dt.timedelta(minutes=2))

        # Start at 12:02 - first window should be 12:02
        results = run_cluster(
            app,
            partition_batch(team_red_data),
            start=BASE + dt.timedelta(minutes=2),
            end=BASE + dt.timedelta(minutes=10),
        )

        window_ends = set()
        for batch in results:
            for i in range(batch.num_rows):
                window_ends.add(batch.column("window_end")[i].as_py())

        assert ts(2, 0) in window_ends  # First window at start time

    @pytest.mark.asyncio
    async def test_last_window_does_not_exceed_end_time(self, run_cluster, team_red_data):
        """Windows past end time are not emitted even if watermark jumps."""
        app = Quackflow()
        app.source("scores", schema=GameSchema, partition_by=["name"])
        app.output(
            "results",
            """
            SELECT COUNT(*) as cnt, window_end
            FROM HOP('scores', 'event_time', INTERVAL '2 minutes')
            GROUP BY window_end
            """,
            schema=WindowedCountSchema,
        ).trigger(window=dt.timedelta(minutes=2))

        # End at 12:06 - should not emit windows after 12:06
        results = run_cluster(
            app,
            partition_batch(team_red_data),
            start=BASE,
            end=BASE + dt.timedelta(minutes=6),
        )

        window_ends = set()
        for batch in results:
            for i in range(batch.num_rows):
                window_ends.add(batch.column("window_end")[i].as_py())

        # Should have windows up to 12:06, but not 12:08
        assert ts(6, 0) in window_ends
        assert ts(8, 0) not in window_ends
