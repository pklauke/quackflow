"""Tests for distributed execution mode."""

import pytest

from quackflow.app import Quackflow

from .conftest import GameSchema, ScoreSchema, TeamScoreSchema, collect_totals, partition_batch


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
