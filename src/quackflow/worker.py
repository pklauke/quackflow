"""Worker - physical execution unit that runs multiple tasks."""

import datetime as dt
from typing import TYPE_CHECKING

from quackflow.app import DAG, OutputDeclaration, SourceDeclaration, ViewDeclaration
from quackflow.execution import ExecutionDAG
from quackflow.task import Task

if TYPE_CHECKING:
    from quackflow.engine import Engine
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
                decl: SourceDeclaration = node.declaration  # type: ignore
                self.engine.create_table(node.name, decl.schema)
            elif node.node_type == "view":
                decl: ViewDeclaration = node.declaration  # type: ignore
                self.engine.create_view(node.name, decl.sql)

        max_window_size = self._compute_max_window_size()

        for task_config in self.exec_dag.tasks.values():
            node = self.user_dag.get_node(task_config.node_name)
            task = Task(task_config, node.declaration, self.engine)

            if isinstance(node.declaration, SourceDeclaration):
                if task_config.node_name in self.sources:
                    task.set_source(self.sources[task_config.node_name])

            if isinstance(node.declaration, OutputDeclaration):
                if task_config.node_name in self.sinks:
                    task.set_sink(self.sinks[task_config.node_name])
                task.set_max_window_size(max_window_size)

            self.tasks[task_config.task_id] = task

        for task_config in self.exec_dag.tasks.values():
            task = self.tasks[task_config.task_id]
            for downstream_id in task_config.downstream_tasks:
                task.downstream_tasks.append(self.tasks[downstream_id])
            for upstream_id in task_config.upstream_tasks:
                task.upstream_tasks.append(self.tasks[upstream_id])

    def _compute_max_window_size(self) -> dt.timedelta:
        all_window_sizes: list[dt.timedelta] = []
        for node in self.user_dag.nodes:
            if node.node_type == "view":
                decl: ViewDeclaration = node.declaration  # type: ignore
                all_window_sizes.extend(decl.window_sizes)
            elif node.node_type == "output":
                decl: OutputDeclaration = node.declaration  # type: ignore
                all_window_sizes.extend(decl.window_sizes)
                if decl.trigger_window is not None:
                    all_window_sizes.append(decl.trigger_window)
        return max(all_window_sizes, default=dt.timedelta(0))

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        self.setup()

        for task in self.tasks.values():
            task.initialize(start)

        for task in self.tasks.values():
            if task.config.node_type == "source":
                await task.run_source(start, end)

        for task in self.tasks.values():
            await task.final_fire()
