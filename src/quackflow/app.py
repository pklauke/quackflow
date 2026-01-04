import datetime as dt
import typing

from quackflow.schema import Schema
from quackflow.source import Source


class SourceConfig:
    def __init__(self, name: str, source: Source, schema: type[Schema]):
        self.name = name
        self.source = source
        self.schema = schema


class ViewConfig:
    def __init__(self, name: str, sql: str, depends_on: list[str], materialize: bool = False):
        self.name = name
        self.sql = sql
        self.depends_on = depends_on
        self.materialize = materialize


SECONDS_PER_DAY = 86400


class OutputConfig:
    def __init__(self, sink: typing.Any, sql: str, schema: type[Schema], depends_on: list[str]):
        self.sink = sink
        self.sql = sql
        self.schema = schema
        self.depends_on = depends_on
        self.trigger_window: dt.timedelta | None = None
        self.trigger_records: int | None = None

    def trigger(
        self,
        window: dt.timedelta | None = None,
        records: int | None = None,
    ) -> "OutputConfig":
        if window is not None:
            window_seconds = int(window.total_seconds())
            if window_seconds <= 0:
                raise ValueError("Window must be positive")
            if SECONDS_PER_DAY % window_seconds != 0:
                raise ValueError(f"Window must divide evenly into a day (86400 seconds), got {window_seconds}s")
        self.trigger_window = window
        self.trigger_records = records
        return self


class Step:
    def __init__(self, name: str, step_type: str, config: SourceConfig | ViewConfig | OutputConfig):
        self.name = name
        self.step_type = step_type
        self.config = config
        self.upstream: list[Step] = []
        self.downstream: list[Step] = []


class DAG:
    def __init__(self):
        self.steps: list[Step] = []
        self._steps_by_name: dict[str, Step] = {}

    def add_step(self, step: Step) -> None:
        self.steps.append(step)
        self._steps_by_name[step.name] = step

    def get_step(self, name: str) -> Step:
        return self._steps_by_name[name]


class Quackflow:
    def __init__(self):
        self.sources: dict[str, SourceConfig] = {}
        self.views: dict[str, ViewConfig] = {}
        self.outputs: list[OutputConfig] = []

    def source(
        self,
        name: str,
        source: Source,
        *,
        schema: type[Schema],
    ) -> None:
        self.sources[name] = SourceConfig(name, source, schema)

    def view(self, name: str, sql: str, *, depends_on: list[str], materialize: bool = False) -> None:
        self.views[name] = ViewConfig(name, sql, depends_on, materialize)

    def output(
        self,
        sink: typing.Any,
        sql: str,
        *,
        schema: type[Schema],
        depends_on: list[str],
    ) -> OutputConfig:
        config = OutputConfig(sink, sql, schema, depends_on)
        self.outputs.append(config)
        return config

    def compile(self) -> DAG:
        for config in self.outputs:
            if config.trigger_window is None and config.trigger_records is None:
                raise ValueError("All outputs must have a trigger (window or records)")

        dag = DAG()

        for name, config in self.sources.items():
            step = Step(name, "source", config)
            dag.add_step(step)

        for name, config in self.views.items():
            step = Step(name, "view", config)
            dag.add_step(step)

            for dep_name in config.depends_on:
                upstream_step = dag.get_step(dep_name)
                step.upstream.append(upstream_step)
                upstream_step.downstream.append(step)

        for i, config in enumerate(self.outputs):
            step = Step(f"output_{i}", "output", config)
            dag.add_step(step)

            for dep_name in config.depends_on:
                upstream_step = dag.get_step(dep_name)
                step.upstream.append(upstream_step)
                upstream_step.downstream.append(step)

        return dag
