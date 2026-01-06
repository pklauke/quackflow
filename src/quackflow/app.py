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


class Node:
    def __init__(self, name: str, node_type: str, config: SourceConfig | ViewConfig | OutputConfig):
        self.name = name
        self.node_type = node_type
        self.config = config
        self.upstream: list[Node] = []
        self.downstream: list[Node] = []
        self._watermark: dt.datetime | None = None  # For source nodes
        self._upstream_watermarks: dict[str, dt.datetime] = {}  # For view/output nodes

    @property
    def effective_watermark(self) -> dt.datetime | None:
        """Effective watermark for this node.

        Returns None until ALL upstream nodes have reported watermarks.
        """
        if self.node_type == "source":
            return self._watermark
        if len(self._upstream_watermarks) < len(self.upstream):
            return None
        return min(self._upstream_watermarks.values())

    def set_watermark(self, watermark: dt.datetime) -> None:
        """Set watermark for source nodes and propagate downstream."""
        self._watermark = watermark
        self._propagate_watermark()

    def receive_watermark(self, upstream_name: str, watermark: dt.datetime) -> None:
        """Receive watermark update from an upstream node."""
        old_effective = self.effective_watermark
        self._upstream_watermarks[upstream_name] = watermark
        new_effective = self.effective_watermark

        # If our effective watermark advanced, propagate downstream
        if new_effective is not None and (old_effective is None or new_effective > old_effective):
            self._propagate_watermark()

    def _propagate_watermark(self) -> None:
        """Propagate this node's effective watermark to all downstream nodes."""
        watermark = self.effective_watermark
        if watermark is not None:
            for downstream in self.downstream:
                downstream.receive_watermark(self.name, watermark)


class DAG:
    def __init__(self):
        self.nodes: list[Node] = []
        self._nodes_by_name: dict[str, Node] = {}

    def add_node(self, node: Node) -> None:
        self.nodes.append(node)
        self._nodes_by_name[node.name] = node

    def get_node(self, name: str) -> Node:
        return self._nodes_by_name[name]

    def connect(self, upstream_name: str, downstream_name: str) -> None:
        upstream = self._nodes_by_name[upstream_name]
        downstream = self._nodes_by_name[downstream_name]
        upstream.downstream.append(downstream)
        downstream.upstream.append(upstream)

    def source_nodes(self) -> list[Node]:
        return [n for n in self.nodes if n.node_type == "source"]

    def output_nodes(self) -> list[Node]:
        return [n for n in self.nodes if n.node_type == "output"]


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
            node = Node(name, "source", config)
            dag.add_node(node)

        for name, config in self.views.items():
            node = Node(name, "view", config)
            dag.add_node(node)

            for dep_name in config.depends_on:
                dag.connect(dep_name, name)

        for i, config in enumerate(self.outputs):
            node = Node(f"output_{i}", "output", config)
            dag.add_node(node)

            for dep_name in config.depends_on:
                dag.connect(dep_name, f"output_{i}")

        return dag
