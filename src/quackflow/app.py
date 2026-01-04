import datetime as dt
import re
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
    def __init__(self, name: str, sql: str, materialize: bool = False):
        self.name = name
        self.sql = sql
        self.materialize = materialize


class OutputConfig:
    def __init__(self, sink: typing.Any, sql: str, schema: type[Schema]):
        self.sink = sink
        self.sql = sql
        self.schema = schema
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

    def view(self, name: str, sql: str, *, materialize: bool = False) -> None:
        self.views[name] = ViewConfig(name, sql, materialize)

    def output(
        self,
        sink: typing.Any,
        sql: str,
        *,
        schema: type[Schema],
    ) -> OutputConfig:
        config = OutputConfig(sink, sql, schema)
        self.outputs.append(config)
        return config

    def compile(self) -> DAG:
        dag = DAG()

        for name, config in self.sources.items():
            step = Step(name, "source", config)
            dag.add_step(step)

        for name, config in self.views.items():
            step = Step(name, "view", config)
            dag.add_step(step)

            for dep_name in self._extract_table_references(config.sql):
                if dep_name in dag._steps_by_name:
                    upstream_step = dag.get_step(dep_name)
                    step.upstream.append(upstream_step)
                    upstream_step.downstream.append(step)

        for i, config in enumerate(self.outputs):
            step = Step(f"output_{i}", "output", config)
            dag.add_step(step)

            for dep_name in self._extract_table_references(config.sql):
                if dep_name in dag._steps_by_name:
                    upstream_step = dag.get_step(dep_name)
                    step.upstream.append(upstream_step)
                    upstream_step.downstream.append(step)

        return dag

    def _extract_table_references(self, sql: str) -> list[str]:
        pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        return matches
