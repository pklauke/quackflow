import datetime as dt

import pyarrow as pa
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


class FakeSource:
    def __init__(self):
        self._watermark: dt.datetime | None = None

    @property
    def watermark(self) -> dt.datetime | None:
        return self._watermark

    async def start(self) -> None:
        pass

    async def read(self) -> pa.RecordBatch:
        return pa.RecordBatch.from_pydict({"id": [], "user_id": [], "event_time": []})

    async def stop(self) -> None:
        pass


class FakeSink:
    def __init__(self):
        self.written: list[pa.RecordBatch] = []

    async def write(self, batch: pa.RecordBatch) -> None:
        self.written.append(batch)


class TestQuackflowRegistration:
    def test_register_source(self):
        app = Quackflow()
        source = FakeSource()

        app.source("events", source, schema=EventSchema)

        assert "events" in app.sources

    def test_register_view(self):
        app = Quackflow()
        source = FakeSource()
        app.source("events", source, schema=EventSchema)

        app.view("user_counts", "SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id", depends_on=["events"])

        assert "user_counts" in app.views

    def test_register_output(self):
        app = Quackflow()
        source = FakeSource()
        sink = FakeSink()
        app.source("events", source, schema=EventSchema)
        app.view("user_counts", "SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id", depends_on=["events"])

        app.output(sink, "SELECT * FROM user_counts", schema=OutputSchema, depends_on=["user_counts"])

        assert len(app.outputs) == 1


class TestQuackflowTrigger:
    def test_output_with_window_trigger(self):
        app = Quackflow()
        source = FakeSource()
        sink = FakeSink()
        app.source("events", source, schema=EventSchema)

        app.output(sink, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(
            window=dt.timedelta(minutes=5)
        )

        assert app.outputs[0].trigger_window == dt.timedelta(minutes=5)

    def test_output_with_records_trigger(self):
        app = Quackflow()
        source = FakeSource()
        sink = FakeSink()
        app.source("events", source, schema=EventSchema)

        app.output(sink, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(records=100)

        assert app.outputs[0].trigger_records == 100

    def test_output_with_combined_trigger(self):
        app = Quackflow()
        source = FakeSource()
        sink = FakeSink()
        app.source("events", source, schema=EventSchema)

        app.output(sink, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(
            window=dt.timedelta(minutes=1), records=10000
        )

        assert app.outputs[0].trigger_window == dt.timedelta(minutes=1)
        assert app.outputs[0].trigger_records == 10000

    def test_window_must_divide_day_evenly(self):
        app = Quackflow()
        source = FakeSource()
        sink = FakeSink()
        app.source("events", source, schema=EventSchema)

        with pytest.raises(ValueError, match="divide evenly"):
            app.output(sink, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(
                window=dt.timedelta(seconds=7)
            )


class TestQuackflowDAG:
    def test_compile_creates_dag(self):
        app = Quackflow()
        source = FakeSource()
        sink = FakeSink()
        app.source("events", source, schema=EventSchema)
        app.view("user_counts", "SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id", depends_on=["events"])
        app.output(sink, "SELECT * FROM user_counts", schema=OutputSchema, depends_on=["user_counts"]).trigger(
            records=1
        )

        dag = app.compile()

        assert dag is not None
        assert len(dag.steps) == 3

    def test_dag_has_correct_dependencies(self):
        app = Quackflow()
        source = FakeSource()
        sink = FakeSink()
        app.source("events", source, schema=EventSchema)
        app.view("user_counts", "SELECT user_id, COUNT(*) as count FROM events GROUP BY user_id", depends_on=["events"])
        app.output(sink, "SELECT * FROM user_counts", schema=OutputSchema, depends_on=["user_counts"]).trigger(
            records=1
        )

        dag = app.compile()

        output_step = dag.get_step("output_0")
        view_step = dag.get_step("user_counts")
        source_step = dag.get_step("events")

        assert view_step in output_step.upstream
        assert source_step in view_step.upstream

    def test_dag_fan_out(self):
        app = Quackflow()
        source = FakeSource()
        sink1 = FakeSink()
        sink2 = FakeSink()
        app.source("events", source, schema=EventSchema)
        app.output(sink1, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(records=1)
        app.output(sink2, "SELECT * FROM events", schema=EventSchema, depends_on=["events"]).trigger(records=1)

        dag = app.compile()

        source_step = dag.get_step("events")
        output1 = dag.get_step("output_0")
        output2 = dag.get_step("output_1")

        assert source_step in output1.upstream
        assert source_step in output2.upstream
        assert output1 in source_step.downstream
        assert output2 in source_step.downstream

    def test_dag_fan_in_join(self):
        app = Quackflow()
        source1 = FakeSource()
        source2 = FakeSource()
        sink = FakeSink()
        app.source("events", source1, schema=EventSchema)
        app.source("users", source2, schema=EventSchema)
        app.view(
            "joined",
            "SELECT * FROM events JOIN users ON events.user_id = users.user_id",
            depends_on=["events", "users"],
        )
        app.output(sink, "SELECT * FROM joined", schema=EventSchema, depends_on=["joined"]).trigger(records=1)

        dag = app.compile()

        events_step = dag.get_step("events")
        users_step = dag.get_step("users")
        joined_step = dag.get_step("joined")

        assert events_step in joined_step.upstream
        assert users_step in joined_step.upstream
