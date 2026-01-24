"""Worker - physical execution unit that runs multiple tasks."""

import asyncio
import datetime as dt
from typing import TYPE_CHECKING

from quackflow.app import DAG, SourceDeclaration, ViewDeclaration
from quackflow._internal.execution import ExecutionDAG
from quackflow._internal.task import Task
from quackflow._internal.transport import LocalDownstreamHandle, LocalUpstreamHandle
from quackflow._internal.worker_utils import compute_max_window_size, create_task

if TYPE_CHECKING:
    from quackflow._internal.engine import Engine
    from quackflow.sink import Sink
    from quackflow.source import Source


class SingleWorkerOrchestrator:
    def __init__(
        self,
        exec_dag: ExecutionDAG,
        user_dag: DAG,
        engine: "Engine",
        sources: dict[str, "Source"],
        sinks: dict[str, "Sink"],
    ):
        self.exec_dag = exec_dag
        self.user_dag = user_dag
        self.engine = engine
        self.sources = sources
        self.sinks = sinks
        self.tasks: dict[str, Task] = {}

    def setup(self) -> None:
        for node in self.user_dag.nodes:
            if node.node_type == "source":
                decl: SourceDeclaration = node.declaration  # type: ignore[assignment]
                self.engine.create_table(node.name, decl.schema)
            elif node.node_type == "view":
                decl: ViewDeclaration = node.declaration  # type: ignore[assignment]
                self.engine.create_view(node.name, decl.sql)

        max_window_size = compute_max_window_size(self.user_dag)

        for task_config in self.exec_dag.tasks.values():
            task = create_task(
                task_config,
                self.user_dag,
                self.engine,
                self.sources,
                self.sinks,
                max_window_size,
            )
            self.tasks[task_config.task_id] = task

        for task_config in self.exec_dag.tasks.values():
            task = self.tasks[task_config.task_id]
            for downstream_id in task_config.downstream_tasks:
                downstream_task = self.tasks[downstream_id]
                task.downstream_handles.append(LocalDownstreamHandle(task.config.task_id, downstream_task))
            for upstream_id in task_config.upstream_tasks:
                upstream_task = self.tasks[upstream_id]
                task.upstream_handles.append(LocalUpstreamHandle(task.config.task_id, upstream_task))

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        self.setup()

        for sink in self.sinks.values():
            await sink.start()

        try:
            for task in self.tasks.values():
                task.initialize(start)

            source_tasks = [
                task.run_source(start, end) for task in self.tasks.values() if task.config.node_type == "source"
            ]
            await asyncio.gather(*source_tasks)

            for task in self.tasks.values():
                await task.final_fire()
        finally:
            for sink in self.sinks.values():
                await sink.stop()
