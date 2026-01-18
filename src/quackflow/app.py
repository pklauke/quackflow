import datetime as dt
from dataclasses import dataclass

from quackflow.schema import Schema
from quackflow.sql import extract_hop_sources, extract_hop_window_sizes, extract_tables, has_group_by


@dataclass
class TriggerConfig:
    """Trigger configuration for when a task should fire."""

    window: dt.timedelta | None = None
    records: int | None = None


# Default records trigger for sources/views (framework-defined batching)
DEFAULT_RECORDS_TRIGGER = 1000


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
        self.trigger: TriggerConfig | None = None  # Set during inference


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
        self.trigger: TriggerConfig | None = None  # Set during inference


SECONDS_PER_DAY = 86400


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
        self.trigger_window: dt.timedelta | None = None
        self.trigger_records: int | None = None

    def trigger(
        self,
        window: dt.timedelta | None = None,
        records: int | None = None,
    ) -> "OutputDeclaration":
        if window is not None:
            window_seconds = int(window.total_seconds())
            if window_seconds <= 0:
                raise ValueError("Window must be positive")
            if SECONDS_PER_DAY % window_seconds != 0:
                raise ValueError(f"Window must divide evenly into a day (86400 seconds), got {window_seconds}s")
        self.trigger_window = window
        self.trigger_records = records
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
    ) -> None:
        self.sources[name] = SourceDeclaration(name, schema, partition_by, ts_col)

    def view(self, name: str, sql: str, *, partition_by: list[str] | None = None) -> None:
        depends_on = self._resolve_dependencies(sql)
        window_sizes = extract_hop_window_sizes(sql)
        self.views[name] = ViewDeclaration(name, sql, depends_on, window_sizes, partition_by)

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
            if declaration.trigger_window is None and declaration.trigger_records is None:
                raise ValueError("All outputs must have a trigger (window or records)")

            if declaration.trigger_window is not None and declaration.window_sizes:
                hop_seconds = int(declaration.trigger_window.total_seconds())
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
                if isinstance(downstream_decl, OutputDeclaration):
                    window = downstream_decl.trigger_window
                else:
                    # Source or View - use inferred trigger
                    window = downstream_decl.trigger.window if downstream_decl.trigger else None

                if window is not None:
                    if min_window is None or window < min_window:
                        min_window = window

            return TriggerConfig(window=min_window, records=DEFAULT_RECORDS_TRIGGER)

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
                node.declaration.trigger = trigger
                processed.add(node.name)

            to_process = [n for n in to_process if n.name not in processed]
