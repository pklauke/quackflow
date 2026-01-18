import pytest

from quackflow.app import Quackflow
from quackflow._internal.execution import ExecutionDAG
from quackflow.schema import Int, Schema, String, Timestamp


class EventSchema(Schema):
    id = Int()
    user_id = String()
    event_time = Timestamp()


class OutputSchema(Schema):
    user_id = String()
    count = Int()


class TestExecutionDAG:
    def test_creates_tasks_per_partition(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(records=1)

        dag = app.compile()
        exec_dag = ExecutionDAG.from_user_dag(dag, num_partitions=2)

        assert len(exec_dag.tasks) == 4  # 2 nodes × 2 partitions
        assert exec_dag.get_task("events[0]").node_name == "events"
        assert exec_dag.get_task("events[1]").node_name == "events"
        assert exec_dag.get_task("results[0]").node_name == "results"
        assert exec_dag.get_task("results[1]").node_name == "results"

    def test_same_partition_connects_without_repartition(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(records=1)

        dag = app.compile()
        exec_dag = ExecutionDAG.from_user_dag(dag, num_partitions=2)

        # Partition 0 connects to partition 0, partition 1 connects to partition 1
        results_0 = exec_dag.get_task("results[0]")
        results_1 = exec_dag.get_task("results[1]")

        assert results_0.upstream_tasks == ["events[0]"]
        assert results_1.upstream_tasks == ["events[1]"]

    def test_repartition_when_partition_key_differs(self):
        app = Quackflow()
        app.source("events", schema=EventSchema, partition_by=["id"])
        app.output(
            "results",
            "SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id",
            schema=OutputSchema,
            partition_by=["user_id"],  # Different key than source
        ).trigger(records=1)

        dag = app.compile()
        exec_dag = ExecutionDAG.from_user_dag(dag, num_partitions=2)

        # All upstream partitions connect to all downstream partitions
        results_0 = exec_dag.get_task("results[0]")
        results_1 = exec_dag.get_task("results[1]")

        assert set(results_0.upstream_tasks) == {"events[0]", "events[1]"}
        assert set(results_1.upstream_tasks) == {"events[0]", "events[1]"}

    def test_repartition_key_set_on_task(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view("filtered", "SELECT * FROM events WHERE user_id = 'alice'", partition_by=["user_id"])
        app.output(
            "results",
            "SELECT user_id, COUNT(*) as count FROM filtered GROUP BY user_id",
            schema=OutputSchema,
        ).trigger(records=1)

        dag = app.compile()
        exec_dag = ExecutionDAG.from_user_dag(dag, num_partitions=2)

        filtered_task = exec_dag.get_task("filtered[0]")
        assert filtered_task.repartition_key == ["user_id"]

    def test_source_and_output_tasks_methods(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view("filtered", "SELECT * FROM events WHERE user_id = 'alice'")
        app.output("results", "SELECT * FROM filtered", schema=EventSchema).trigger(records=1)

        dag = app.compile()
        exec_dag = ExecutionDAG.from_user_dag(dag, num_partitions=2)

        source_tasks = exec_dag.source_tasks()
        output_tasks = exec_dag.output_tasks()

        assert len(source_tasks) == 2
        assert all(t.node_type == "source" for t in source_tasks)

        assert len(output_tasks) == 2
        assert all(t.node_type == "output" for t in output_tasks)

    def test_multi_stage_repartition(self):
        """Test repartitioning between view and output."""
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view("joined", "SELECT * FROM events", partition_by=["id"])
        app.output(
            "results",
            "SELECT user_id, COUNT(*) as count FROM joined GROUP BY user_id",
            schema=OutputSchema,
            partition_by=["user_id"],
        ).trigger(records=1)

        dag = app.compile()
        exec_dag = ExecutionDAG.from_user_dag(dag, num_partitions=2)

        # events[p] → joined[p] (no repartition, joined has partition_by but source doesn't)
        # Actually, joined[p] gets all events[*] because source has no partition_by but joined has one
        joined_0 = exec_dag.get_task("joined[0]")
        assert set(joined_0.upstream_tasks) == {"events[0]", "events[1]"}

        # joined[*] → results[p] (repartition because different keys)
        results_0 = exec_dag.get_task("results[0]")
        assert set(results_0.upstream_tasks) == {"joined[0]", "joined[1]"}
