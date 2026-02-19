"""Worker - physical execution unit that runs multiple tasks."""

import abc
import asyncio
import datetime as dt
from typing import TYPE_CHECKING, Any

from quackflow.app import DAG
from quackflow._internal.execution import ExecutionDAG, TaskConfig
from quackflow._internal.task import Task
from quackflow._internal.transport import (
    DownstreamHandle,
    LocalDownstreamHandle,
    LocalUpstreamHandle,
    UpstreamHandle,
)
from quackflow._internal.worker_utils import (
    compute_max_window_size,
    compute_max_window_size_per_source,
    create_task,
)

if TYPE_CHECKING:
    from quackflow._internal.engine import Engine
    from quackflow.sink import Sink
    from quackflow.source import Source


class BaseOrchestrator(abc.ABC):
    """Base class for single-worker and distributed orchestrators.

    Uses the template method pattern: shared setup/run logic with
    abstract methods for what differs between deployment modes.
    """

    def __init__(
        self,
        exec_dag: ExecutionDAG,
        user_dag: DAG,
        sources: dict[str, "Source"],
        sinks: dict[str, "Sink"],
    ):
        self.exec_dag = exec_dag
        self.user_dag = user_dag
        self.sources = sources
        self.sinks = sinks
        self.tasks: dict[str, Task] = {}

    # -- Abstract methods (subclasses must implement) --

    @abc.abstractmethod
    def _get_task_ids(self) -> list[str]:
        """Return the task IDs this orchestrator is responsible for."""

    @abc.abstractmethod
    def _get_engine(self, task_config: TaskConfig) -> "Engine":
        """Return the engine to use for a given task."""

    @abc.abstractmethod
    def _create_downstream_handle(self, sender_id: str, target_id: str) -> DownstreamHandle:
        """Create a handle for sending data to a downstream task."""

    @abc.abstractmethod
    def _create_upstream_handle(self, sender_id: str, target_id: str) -> UpstreamHandle:
        """Create a handle for sending expiration to an upstream task."""

    # -- Optional hooks (subclasses may override) --

    def _setup_engines(self) -> None:
        """Called before task creation. Override to set up shared engine state."""

    def _extra_task_kwargs(self) -> dict[str, Any]:
        """Extra keyword arguments passed to create_task."""
        return {}

    async def _before_execute(self) -> None:
        """Called after setup but before executing tasks. Override for startup hooks."""

    async def _cleanup(self) -> None:
        """Called during finally block after task execution. Override for cleanup."""

    # -- Shared setup/run logic --

    def setup(self) -> None:
        self._setup_engines()

        max_window_size = compute_max_window_size(self.user_dag)
        source_window_sizes = compute_max_window_size_per_source(self.user_dag)
        extra_kwargs = self._extra_task_kwargs()

        for task_id in self._get_task_ids():
            task_config = self.exec_dag.get_task(task_id)
            engine = self._get_engine(task_config)
            task = create_task(
                task_config,
                self.user_dag,
                engine,
                self.sources,
                self.sinks,
                max_window_size,
                source_window_sizes,
                **extra_kwargs,
            )
            self.tasks[task_id] = task

        for task_id in self._get_task_ids():
            task_config = self.exec_dag.get_task(task_id)
            task = self.tasks[task_id]

            for downstream_id in task_config.downstream_tasks:
                task.downstream_handles.append(self._create_downstream_handle(task_config.task_id, downstream_id))

            for upstream_id in task_config.upstream_tasks:
                task.upstream_handles.append(self._create_upstream_handle(task_config.task_id, upstream_id))

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        self.setup()
        await self._before_execute()

        for sink in self.sinks.values():
            await sink.start()

        try:
            for task in self.tasks.values():
                task.initialize(start, end)

            source_tasks = [
                task.run_source(start, end) for task in self.tasks.values() if task.config.node_type == "source"
            ]
            await asyncio.gather(*source_tasks)

            for task in self.tasks.values():
                await task.final_fire()
        finally:
            for sink in self.sinks.values():
                await sink.stop()
            await self._cleanup()


class SingleWorkerOrchestrator(BaseOrchestrator):
    def __init__(
        self,
        exec_dag: ExecutionDAG,
        user_dag: DAG,
        engine: "Engine",
        sources: dict[str, "Source"],
        sinks: dict[str, "Sink"],
    ):
        super().__init__(exec_dag, user_dag, sources, sinks)
        self.engine = engine

    def _get_task_ids(self) -> list[str]:
        return list(self.exec_dag.tasks.keys())

    def _get_engine(self, task_config: TaskConfig) -> "Engine":
        return self.engine

    def _setup_engines(self) -> None:
        from quackflow.app import SourceDeclaration, ViewDeclaration

        for node in self.user_dag.nodes:
            decl = node.declaration
            if isinstance(decl, SourceDeclaration):
                self.engine.create_table(node.name, decl.schema)
            elif isinstance(decl, ViewDeclaration):
                self.engine.create_view(node.name, decl.sql)

    def _create_downstream_handle(self, sender_id: str, target_id: str) -> DownstreamHandle:
        return LocalDownstreamHandle(sender_id, self.tasks[target_id])

    def _create_upstream_handle(self, sender_id: str, target_id: str) -> UpstreamHandle:
        return LocalUpstreamHandle(sender_id, self.tasks[target_id])
