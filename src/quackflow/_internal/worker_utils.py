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


def create_task(
    task_config: TaskConfig,
    user_dag: DAG,
    engine: "Engine",
    sources: dict[str, "Source"],
    sinks: dict[str, "Sink"],
    max_window_size: dt.timedelta,
    *,
    num_partitions: int = 1,
    propagate_batch: bool = False,
) -> Task:
    """Create a Task with all necessary settings."""
    node = user_dag.get_node(task_config.node_name)
    declaration = node.declaration

    source = sources.get(task_config.node_name) if isinstance(declaration, SourceDeclaration) else None
    sink = sinks.get(task_config.node_name) if isinstance(declaration, OutputDeclaration) else None
    # Sources need max_window_size for seek adjustment, outputs need it for batch window calculation
    task_max_window = (
        max_window_size if isinstance(declaration, (SourceDeclaration, OutputDeclaration)) else dt.timedelta(0)
    )

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
