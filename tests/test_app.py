import datetime as dt

import pytest

from quackflow.app import Quackflow
from quackflow.schema import Int, Schema, String, Timestamp


class EventSchema(Schema):
    id = Int()
    user_id = String()
    event_time = Timestamp()


class OutputSchema(Schema):
    user_id = String()
    count = Int()


class TestQuackflowRegistration:
    def test_register_source(self):
        app = Quackflow()

        app.source("events", schema=EventSchema)

        assert "events" in app.sources

    def test_register_view(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)

        app.view("user_counts", "SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id")

        assert "user_counts" in app.views

    def test_register_output(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view("user_counts", "SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id")

        app.output("results", "SELECT * FROM user_counts", schema=OutputSchema)

        assert "results" in app.outputs


class TestQuackflowTrigger:
    def test_output_with_window_trigger(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)

        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(window=dt.timedelta(minutes=5))

        assert app.outputs["results"].trigger_window == dt.timedelta(minutes=5)

    def test_output_with_records_trigger(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)

        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(records=100)

        assert app.outputs["results"].trigger_records == 100

    def test_output_with_combined_trigger(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)

        app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(
            window=dt.timedelta(minutes=1), records=10000
        )

        assert app.outputs["results"].trigger_window == dt.timedelta(minutes=1)
        assert app.outputs["results"].trigger_records == 10000

    def test_window_must_divide_day_evenly(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)

        with pytest.raises(ValueError, match="divide evenly"):
            app.output("results", "SELECT * FROM events", schema=EventSchema).trigger(window=dt.timedelta(seconds=7))

    def test_hop_window_size_must_be_multiple_of_trigger(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output(
            "results",
            "SELECT * FROM HOP('events', 'event_time', INTERVAL '10 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=3))

        with pytest.raises(ValueError, match="must be a multiple"):
            app.compile()

    def test_hop_window_size_valid_when_multiple(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output(
            "results",
            "SELECT * FROM HOP('events', 'event_time', INTERVAL '10 minutes')",
            schema=EventSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        dag = app.compile()
        assert dag is not None

    def test_view_with_group_by_not_allowed(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view(
            "user_counts",
            "SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id",
        )
        app.output("results", "SELECT * FROM user_counts", schema=EventSchema).trigger(records=1)

        with pytest.raises(ValueError, match="GROUP BY"):
            app.compile()


class TestQuackflowDAG:
    def test_compile_creates_dag(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view("filtered_events", "SELECT * FROM events WHERE user_id = 'alice'")
        app.output(
            "results", "SELECT user_id, COUNT(*) as count FROM filtered_events GROUP BY user_id", schema=OutputSchema
        ).trigger(records=1)

        dag = app.compile()

        assert dag is not None
        assert len(dag.nodes) == 3

    def test_dag_has_correct_dependencies(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.view("filtered_events", "SELECT * FROM events WHERE user_id = 'alice'")
        app.output(
            "results", "SELECT user_id, COUNT(*) as count FROM filtered_events GROUP BY user_id", schema=OutputSchema
        ).trigger(records=1)

        dag = app.compile()

        output_node = dag.get_node("results")
        view_node = dag.get_node("filtered_events")
        source_node = dag.get_node("events")

        assert view_node in output_node.upstream
        assert source_node in view_node.upstream

    def test_dag_fan_out(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.output("output1", "SELECT * FROM events", schema=EventSchema).trigger(records=1)
        app.output("output2", "SELECT * FROM events", schema=EventSchema).trigger(records=1)

        dag = app.compile()

        source_node = dag.get_node("events")
        output1 = dag.get_node("output1")
        output2 = dag.get_node("output2")

        assert source_node in output1.upstream
        assert source_node in output2.upstream
        assert output1 in source_node.downstream
        assert output2 in source_node.downstream

    def test_dag_fan_in_join(self):
        app = Quackflow()
        app.source("events", schema=EventSchema)
        app.source("users", schema=EventSchema)
        app.view(
            "joined",
            "SELECT * FROM events JOIN users ON events.user_id = users.user_id",
        )
        app.output("results", "SELECT * FROM joined", schema=EventSchema).trigger(records=1)

        dag = app.compile()

        events_node = dag.get_node("events")
        users_node = dag.get_node("users")
        joined_node = dag.get_node("joined")

        assert events_node in joined_node.upstream
        assert users_node in joined_node.upstream
