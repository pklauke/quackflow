import datetime as dt
import typing
from dataclasses import dataclass

import pyarrow as pa

from quackflow.schema import Schema
from quackflow.source import Source


@dataclass
class DataPacket:
    batch: pa.RecordBatch
    watermark: dt.datetime


class SourceBinding:
    def __init__(self, name: str, source: Source, schema: type[Schema]):
        self.name = name
        self.source = source
        self.schema = schema


class ViewBinding:
    def __init__(self, name: str, sql: str, depends_on: list[str], materialize: bool = False):
        self.name = name
        self.sql = sql
        self.depends_on = depends_on
        self.materialize = materialize


SECONDS_PER_DAY = 86400


class OutputBinding:
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
    ) -> "OutputBinding":
        if window is not None:
            window_seconds = int(window.total_seconds())
            if window_seconds <= 0:
                raise ValueError("Window must be positive")
            if SECONDS_PER_DAY % window_seconds != 0:
                raise ValueError(f"Window must divide evenly into a day (86400 seconds), got {window_seconds}s")
        self.trigger_window = window
        self.trigger_records = records
        return self


OnAdvanceCallback = typing.Callable[[], typing.Awaitable[None]]


class Node:
    def __init__(self, name: str, node_type: str, binding: SourceBinding | ViewBinding | OutputBinding):
        self.name = name
        self.node_type = node_type
        self.binding = binding
        self.upstream: list[Node] = []
        self.downstream: list[Node] = []
        self._watermark: dt.datetime | None = None  # For source nodes
        self._upstream_watermarks: dict[str, dt.datetime] = {}  # For view/output nodes
        self._upstream_records: dict[str, int] = {}  # Records received per upstream
        self._on_advance: OnAdvanceCallback | None = None

    def set_on_advance_callback(self, callback: OnAdvanceCallback) -> None:
        """Register a callback to be called when effective watermark advances."""
        self._on_advance = callback

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

    @property
    def total_records(self) -> int:
        """Total records received from all upstream nodes."""
        return sum(self._upstream_records.values())

    async def send(self, packet: DataPacket) -> None:
        """Send a data packet downstream (for source nodes)."""
        self._watermark = packet.watermark
        await self._propagate(packet)

    async def receive(self, upstream_name: str, packet: DataPacket) -> None:
        """Receive a data packet from an upstream node."""
        old_effective = self.effective_watermark
        self._upstream_watermarks[upstream_name] = packet.watermark
        self._upstream_records[upstream_name] = self._upstream_records.get(upstream_name, 0) + packet.batch.num_rows
        new_effective = self.effective_watermark

        # If our effective watermark advanced, notify callback and propagate downstream
        if new_effective is not None and (old_effective is None or new_effective > old_effective):
            if self._on_advance is not None:
                await self._on_advance()
            await self._propagate(packet)

    async def _propagate(self, packet: DataPacket) -> None:
        """Propagate a data packet to all downstream nodes."""
        for downstream in self.downstream:
            await downstream.receive(self.name, packet)


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
        self.sources: dict[str, SourceBinding] = {}
        self.views: dict[str, ViewBinding] = {}
        self.outputs: list[OutputBinding] = []

    def source(
        self,
        name: str,
        source: Source,
        *,
        schema: type[Schema],
    ) -> None:
        self.sources[name] = SourceBinding(name, source, schema)

    def view(self, name: str, sql: str, *, depends_on: list[str], materialize: bool = False) -> None:
        self.views[name] = ViewBinding(name, sql, depends_on, materialize)

    def output(
        self,
        sink: typing.Any,
        sql: str,
        *,
        schema: type[Schema],
        depends_on: list[str],
    ) -> OutputBinding:
        binding = OutputBinding(sink, sql, schema, depends_on)
        self.outputs.append(binding)
        return binding

    def compile(self) -> DAG:
        for binding in self.outputs:
            if binding.trigger_window is None and binding.trigger_records is None:
                raise ValueError("All outputs must have a trigger (window or records)")

        dag = DAG()

        for name, binding in self.sources.items():
            node = Node(name, "source", binding)
            dag.add_node(node)

        for name, binding in self.views.items():
            node = Node(name, "view", binding)
            dag.add_node(node)

            for dep_name in binding.depends_on:
                dag.connect(dep_name, name)

        for i, binding in enumerate(self.outputs):
            node = Node(f"output_{i}", "output", binding)
            dag.add_node(node)

            for dep_name in binding.depends_on:
                dag.connect(dep_name, f"output_{i}")

        return dag
