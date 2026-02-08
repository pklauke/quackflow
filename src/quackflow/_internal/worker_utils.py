"""Shared utilities for worker orchestrators."""

import datetime as dt
from typing import TYPE_CHECKING

from quackflow.app import DAG, OutputDeclaration, SourceDeclaration, ViewDeclaration
from quackflow._internal.execution import TaskConfig
from quackflow._internal.task import Task

if TYPE_CHECKING:
    from quackflow._internal.engine import Engine
    from quackflow.sink import Sink
    from quackflow.source import Source


def compute_max_window_size(user_dag: DAG) -> dt.timedelta:
    """Compute the maximum window size across all views and outputs."""
    all_window_sizes: list[dt.timedelta] = []
    for node in user_dag.nodes:
        if node.node_type == "view":
            decl: ViewDeclaration = node.declaration  # type: ignore[assignment]
            all_window_sizes.extend(decl.window_sizes)
        elif node.node_type == "output":
            decl: OutputDeclaration = node.declaration  # type: ignore[assignment]
            all_window_sizes.extend(decl.window_sizes)
            if decl._trigger is not None and decl._trigger.window is not None:
                all_window_sizes.append(decl._trigger.window)
    return max(all_window_sizes, default=dt.timedelta(0))


def compute_max_window_size_per_source(user_dag: DAG) -> dict[str, dt.timedelta]:
    """Compute the maximum window size for each source based on downstream HOPs that use its data."""
    from quackflow._internal.sql import extract_hop_sources

    result: dict[str, dt.timedelta] = {}

    # Build reverse dependency map: for each table, which sources does it depend on?
    # (including itself if it's a source)
    sources_for_table: dict[str, set[str]] = {}

    def get_upstream_sources(table_name: str, visited: set[str] | None = None) -> set[str]:
        if visited is None:
            visited = set()
        if table_name in visited:
            return set()
        visited.add(table_name)

        if table_name in sources_for_table:
            return sources_for_table[table_name]

        node = user_dag.get_node(table_name)
        if node.node_type == "source":
            sources_for_table[table_name] = {table_name}
            return {table_name}

        # For views/outputs, collect sources from all dependencies
        decl = node.declaration
        upstream_sources: set[str] = set()
        if hasattr(decl, "depends_on"):
            for dep in decl.depends_on:
                upstream_sources.update(get_upstream_sources(dep, visited))

        sources_for_table[table_name] = upstream_sources
        return upstream_sources

    # Populate sources_for_table for all nodes
    for node in user_dag.nodes:
        get_upstream_sources(node.name)

    # Now collect window sizes per source by looking at all HOPs in views/outputs
    source_window_sizes: dict[str, list[dt.timedelta]] = {
        node.name: [] for node in user_dag.nodes if node.node_type == "source"
    }

    for node in user_dag.nodes:
        if node.node_type == "source":
            continue

        decl = node.declaration
        sql = getattr(decl, "sql", None)
        if not sql:
            continue

        # Extract {hop_table: (ts_col, window_size)} from this node's SQL
        hop_sources = extract_hop_sources(sql)
        for hop_table, (_, window_size) in hop_sources.items():
            # Find which sources this HOP table depends on
            upstream_sources = sources_for_table.get(hop_table, set())
            for source_name in upstream_sources:
                if source_name in source_window_sizes:
                    source_window_sizes[source_name].append(window_size)

        # Also add trigger window for outputs (applies to all upstream sources)
        if node.node_type == "output":
            output_decl: OutputDeclaration = decl  # type: ignore[assignment]
            if output_decl._trigger is not None and output_decl._trigger.window is not None:
                for dep in output_decl.depends_on:
                    for source_name in sources_for_table.get(dep, set()):
                        if source_name in source_window_sizes:
                            source_window_sizes[source_name].append(output_decl._trigger.window)

    for source_name, sizes in source_window_sizes.items():
        result[source_name] = max(sizes, default=dt.timedelta(0))

    return result


def create_task(
    task_config: TaskConfig,
    user_dag: DAG,
    engine: "Engine",
    sources: dict[str, "Source"],
    sinks: dict[str, "Sink"],
    max_window_size: dt.timedelta,
    source_window_sizes: dict[str, dt.timedelta],
    *,
    num_partitions: int = 1,
    propagate_batch: bool = False,
) -> Task:
    """Create a Task with all necessary settings."""
    node = user_dag.get_node(task_config.node_name)
    declaration = node.declaration

    source = sources.get(task_config.node_name) if isinstance(declaration, SourceDeclaration) else None
    sink = sinks.get(task_config.node_name) if isinstance(declaration, OutputDeclaration) else None

    # Determine max window size for this task
    if isinstance(declaration, SourceDeclaration):
        # Sources use per-source window size for seek adjustment
        task_max_window = source_window_sizes.get(task_config.node_name, dt.timedelta(0))
    elif isinstance(declaration, ViewDeclaration):
        # Views use their own max window size for HOP functions
        task_max_window = max(declaration.window_sizes, default=dt.timedelta(0))
    elif isinstance(declaration, OutputDeclaration):
        # Outputs use their own window sizes if they have HOP,
        # otherwise use global max (for outputs that reference views with HOP)
        own_max = max(declaration.window_sizes, default=dt.timedelta(0))
        task_max_window = own_max if own_max > dt.timedelta(0) else max_window_size
    else:
        task_max_window = dt.timedelta(0)

    return Task(
        config=task_config,
        declaration=declaration,
        engine=engine,
        source=source,
        sink=sink,
        max_window_size=task_max_window,
        num_partitions=num_partitions,
        propagate_batch=propagate_batch,
    )
