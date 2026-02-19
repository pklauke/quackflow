import datetime as dt
from dataclasses import dataclass

from quackflow.schema import Schema
from quackflow._internal.sql import extract_hop_sources, extract_hop_window_sizes, extract_tables, has_group_by

DEFAULT_RECORDS_TRIGGER = 1000
SECONDS_PER_DAY = 86400


@dataclass
class TriggerConfig:
    """Trigger configuration for when a task should fire."""

    window: dt.timedelta | None = None
    records: int | None = None


class SourceDeclaration:
    def __init__(
        self,
        name: str,
        schema: type[Schema],
        partition_by: list[str] | None = None,
        ts_col: str | None = None,
    ):
        self.name = name
        self.schema = schema
        self.partition_by = partition_by
        self.ts_col = ts_col
        self._trigger: TriggerConfig | None = None  # Set during inference


class ViewDeclaration:
    def __init__(
        self,
        name: str,
        sql: str,
        depends_on: list[str],
        window_sizes: list[dt.timedelta],
        partition_by: list[str] | None = None,
    ):
        self.name = name
        self.sql = sql
        self.depends_on = depends_on
        self.window_sizes = window_sizes
        self.partition_by = partition_by
        self._trigger: TriggerConfig | None = None  # Set during inference


class OutputDeclaration:
    def __init__(
        self,
        name: str,
        sql: str,
        schema: type[Schema],
        depends_on: list[str],
        window_sizes: list[dt.timedelta],
        partition_by: list[str] | None = None,
    ):
        self.name = name
        self.sql = sql
        self.schema = schema
        self.depends_on = depends_on
        self.window_sizes = window_sizes
        self.partition_by = partition_by
        self._trigger: TriggerConfig | None = None

    def trigger(
        self,
        window: dt.timedelta | None = None,
        records: int | None = None,
    ) -> "OutputDeclaration":
        if records is not None and self.window_sizes:
            raise ValueError("Record triggers cannot be used with HOP-based queries. Use a window trigger instead.")
        if window is not None:
            window_seconds = int(window.total_seconds())
            if window_seconds <= 0:
                raise ValueError("Window must be positive")
            if SECONDS_PER_DAY % window_seconds != 0:
                raise ValueError(
                    f"Window of {window_seconds}s doesn't divide evenly into 24 hours. "
                    f"Windows align to midnight boundaries (e.g., 00:00, 00:05, 00:10 for 5-minute windows), "
                    f"so window duration must divide 86400 seconds evenly. "
                    f"Try a window like 1s, 5s, 10s, 15s, 30s, 1min, 5min, 10min, 15min, 30min, or 1h."
                )
        self._trigger = TriggerConfig(window=window, records=records)
        return self


Declaration = SourceDeclaration | ViewDeclaration | OutputDeclaration


class Node:
    def __init__(self, name: str, node_type: str, declaration: Declaration):
        self.name = name
        self.node_type = node_type
        self.declaration = declaration
        self.upstream: list[Node] = []
        self.downstream: list[Node] = []


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

    def output_nodes(self) -> list[Node]:
        return [n for n in self.nodes if n.node_type == "output"]


class Quackflow:
    def __init__(self):
        self.sources: dict[str, SourceDeclaration] = {}
        self.views: dict[str, ViewDeclaration] = {}
        self.outputs: dict[str, OutputDeclaration] = {}

    def source(
        self,
        name: str,
        *,
        schema: type[Schema],
        partition_by: list[str] | None = None,
        ts_col: str | None = None,
    ) -> SourceDeclaration:
        declaration = SourceDeclaration(name, schema, partition_by, ts_col)
        self.sources[name] = declaration
        return declaration

    def view(self, name: str, sql: str, *, partition_by: list[str] | None = None) -> ViewDeclaration:
        depends_on = self._resolve_dependencies(sql)
        window_sizes = extract_hop_window_sizes(sql)
        declaration = ViewDeclaration(name, sql, depends_on, window_sizes, partition_by)
        self.views[name] = declaration
        return declaration

    def output(
        self,
        name: str,
        sql: str,
        *,
        schema: type[Schema],
        partition_by: list[str] | None = None,
    ) -> OutputDeclaration:
        depends_on = self._resolve_dependencies(sql)
        window_sizes = extract_hop_window_sizes(sql)
        declaration = OutputDeclaration(name, sql, schema, depends_on, window_sizes, partition_by)
        self.outputs[name] = declaration
        return declaration

    def _resolve_dependencies(self, sql: str) -> list[str]:
        referenced = extract_tables(sql)
        known = set(self.sources.keys()) | set(self.views.keys())
        return list(referenced & known)

    def _check_stacked_aggregations(self) -> None:
        for view in self.views.values():
            hop_sources = extract_hop_sources(view.sql)
            for source_name, (ts_col, _) in hop_sources.items():
                if source_name in self.views and ts_col == "window_end":
                    upstream_view = self.views[source_name]
                    if upstream_view.window_sizes:
                        raise ValueError(
                            f"Stacked aggregations not supported: view '{view.name}' uses HOP on "
                            f"'{source_name}.window_end', but '{source_name}' already contains a HOP"
                        )

        for output in self.outputs.values():
            hop_sources = extract_hop_sources(output.sql)
            for source_name, (ts_col, _) in hop_sources.items():
                if source_name in self.views and ts_col == "window_end":
                    upstream_view = self.views[source_name]
                    if upstream_view.window_sizes:
                        raise ValueError(
                            f"Stacked aggregations not supported: output '{output.name}' uses HOP on "
                            f"'{source_name}.window_end', but '{source_name}' already contains a HOP"
                        )

    def compile(self) -> DAG:
        for declaration in self.outputs.values():
            if declaration._trigger is None:
                raise ValueError("All outputs must have a trigger (window or records)")

            if declaration._trigger.window is not None and declaration.window_sizes:
                hop_seconds = int(declaration._trigger.window.total_seconds())
                for window_size in declaration.window_sizes:
                    size_seconds = int(window_size.total_seconds())
                    if size_seconds % hop_seconds != 0:
                        raise ValueError(
                            f"Window size ({size_seconds}s) must be a multiple of trigger window ({hop_seconds}s)"
                        )

        # Validate: views cannot have GROUP BY (aggregation only in output)
        for view in self.views.values():
            if has_group_by(view.sql):
                raise ValueError(
                    f"View '{view.name}' contains GROUP BY. Aggregations are only allowed in output nodes."
                )

        self._check_stacked_aggregations()

        dag = DAG()

        for name, declaration in self.sources.items():
            node = Node(name, "source", declaration)
            dag.add_node(node)

        for name, declaration in self.views.items():
            node = Node(name, "view", declaration)
            dag.add_node(node)

            for dep_name in declaration.depends_on:
                dag.connect(dep_name, name)

        for name, declaration in self.outputs.items():
            node = Node(name, "output", declaration)
            dag.add_node(node)

            for dep_name in declaration.depends_on:
                dag.connect(dep_name, name)

        self._infer_triggers(dag)

        return dag

    def _infer_triggers(self, dag: DAG) -> None:
        """Infer triggers for sources and views based on downstream outputs."""
        # Process nodes in reverse topological order (outputs first, then upstream)
        # Using BFS from outputs going upstream
        processed: set[str] = set()

        def get_trigger_from_downstream(node: Node) -> TriggerConfig:
            """Compute trigger config based on downstream requirements."""
            min_window: dt.timedelta | None = None

            for downstream in node.downstream:
                downstream_decl = downstream.declaration

                # Get downstream's window trigger
                window = downstream_decl._trigger.window if downstream_decl._trigger else None

                if window is not None:
                    if min_window is None or window < min_window:
                        min_window = window

            # Nodes with HOP (window_sizes) should only use window triggers, not record triggers
            # Record triggers don't make sense for windowed aggregations
            decl = node.declaration
            has_hop = isinstance(decl, ViewDeclaration) and len(decl.window_sizes) > 0
            records_trigger = None if has_hop else DEFAULT_RECORDS_TRIGGER

            return TriggerConfig(window=min_window, records=records_trigger)

        # Process outputs first (they have user-defined triggers, no inference needed)
        for node in dag.output_nodes():
            processed.add(node.name)

        # Process remaining nodes layer by layer, going upstream
        to_process = [n for n in dag.nodes if n.name not in processed]
        while to_process:
            # Find nodes whose downstream are all processed
            ready = [n for n in to_process if all(d.name in processed for d in n.downstream)]

            for node in ready:
                trigger = get_trigger_from_downstream(node)
                node.declaration._trigger = trigger
                processed.add(node.name)

            to_process = [n for n in to_process if n.name not in processed]
