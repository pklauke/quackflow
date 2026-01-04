import datetime as dt
import typing

from quackflow.schema import Schema
from quackflow.source import Source
from quackflow.time_notion import TimeNotion


class SourceConfig:
    def __init__(self, name: str, source: Source, schema: type[Schema], time_notion: TimeNotion):
        self.name = name
        self.source = source
        self.schema = schema
        self.time_notion = time_notion


class ViewConfig:
    def __init__(self, name: str, sql: str, depends_on: list[str], materialize: bool = False):
        self.name = name
        self.sql = sql
        self.depends_on = depends_on
        self.materialize = materialize


class OutputConfig:
    def __init__(self, sink: typing.Any, sql: str, schema: type[Schema], depends_on: list[str]):
        self.sink = sink
        self.sql = sql
        self.schema = schema
        self.depends_on = depends_on
        self.trigger_interval: dt.timedelta | None = None
        self.trigger_records: int | None = None

    def trigger(
        self,
        interval: dt.timedelta | None = None,
        records: int | None = None,
    ) -> "OutputConfig":
        self.trigger_interval = interval
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
        time_notion: TimeNotion,
    ) -> None:
        self.sources[name] = SourceConfig(name, source, schema, time_notion)

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
            if config.trigger_interval is None and config.trigger_records is None:
                raise ValueError("All outputs must have a trigger (interval or records)")

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
