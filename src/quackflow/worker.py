"""Worker - physical execution unit that runs multiple tasks."""

import asyncio
import datetime as dt
from typing import TYPE_CHECKING

from quackflow.app import DAG, OutputDeclaration, SourceDeclaration, ViewDeclaration
from quackflow.execution import ExecutionDAG, TaskConfig
from quackflow.task import Task

if TYPE_CHECKING:
    from quackflow.engine import Engine
    from quackflow.sink import Sink
    from quackflow.source import Source


class Worker:
    """Runs multiple tasks and manages inter-task communication."""

    def __init__(
        self,
        worker_id: int,
        exec_dag: ExecutionDAG,
        user_dag: DAG,
        engine: "Engine",
        sources: dict[str, "Source"],
        sinks: dict[str, "Sink"],
        task_configs: list[TaskConfig],
    ):
        self.worker_id = worker_id
        self.exec_dag = exec_dag
        self.user_dag = user_dag
        self.engine = engine
        self.sources = sources
        self.sinks = sinks
        self.task_configs = task_configs

        # Tasks and queues (created during setup)
        self.tasks: dict[str, Task] = {}
        self.queues: dict[str, "asyncio.Queue"] = {}
        self.expiration_queues: dict[str, "asyncio.Queue"] = {}  # Backward direction

    def setup(self, max_window_size: dt.timedelta) -> None:
        """Set up tasks and inter-task queues."""
        # Create data queues (forward direction: upstream → downstream)
        for task_config in self.task_configs:
            for upstream_id in task_config.upstream_tasks:
                queue_key = f"{upstream_id}->{task_config.task_id}"
                if queue_key not in self.queues:
                    self.queues[queue_key] = asyncio.Queue()

            for downstream_id in task_config.downstream_tasks:
                queue_key = f"{task_config.task_id}->{downstream_id}"
                if queue_key not in self.queues:
                    self.queues[queue_key] = asyncio.Queue()

        # Create expiration queues (backward direction: downstream → upstream)
        for task_config in self.task_configs:
            for downstream_id in task_config.downstream_tasks:
                # Expiration flows from downstream to this task
                exp_queue_key = f"{downstream_id}->exp->{task_config.task_id}"
                if exp_queue_key not in self.expiration_queues:
                    self.expiration_queues[exp_queue_key] = asyncio.Queue()

            for upstream_id in task_config.upstream_tasks:
                # This task sends expiration to upstream
                exp_queue_key = f"{task_config.task_id}->exp->{upstream_id}"
                if exp_queue_key not in self.expiration_queues:
                    self.expiration_queues[exp_queue_key] = asyncio.Queue()

        # Create tasks
        for task_config in self.task_configs:
            # Get the declaration from user DAG
            node = self.user_dag.get_node(task_config.node_name)
            declaration = node.declaration

            # Build input/output queue maps for this task (data flows forward)
            input_queues: dict[str, asyncio.Queue] = {}
            for upstream_id in task_config.upstream_tasks:
                queue_key = f"{upstream_id}->{task_config.task_id}"
                if queue_key in self.queues:
                    input_queues[upstream_id] = self.queues[queue_key]

            output_queues: dict[str, asyncio.Queue] = {}
            for downstream_id in task_config.downstream_tasks:
                queue_key = f"{task_config.task_id}->{downstream_id}"
                if queue_key in self.queues:
                    output_queues[downstream_id] = self.queues[queue_key]

            # Build expiration queue maps (expiration flows backward)
            # expiration_input: receive from downstream
            expiration_input_queues: dict[str, asyncio.Queue] = {}
            for downstream_id in task_config.downstream_tasks:
                exp_queue_key = f"{downstream_id}->exp->{task_config.task_id}"
                if exp_queue_key in self.expiration_queues:
                    expiration_input_queues[downstream_id] = self.expiration_queues[exp_queue_key]

            # expiration_output: send to upstream
            expiration_output_queues: dict[str, asyncio.Queue] = {}
            for upstream_id in task_config.upstream_tasks:
                exp_queue_key = f"{task_config.task_id}->exp->{upstream_id}"
                if exp_queue_key in self.expiration_queues:
                    expiration_output_queues[upstream_id] = self.expiration_queues[exp_queue_key]

            task = Task(
                config=task_config,
                declaration=declaration,
                engine=self.engine,
                input_queues=input_queues,
                output_queues=output_queues,
                expiration_input_queues=expiration_input_queues,
                expiration_output_queues=expiration_output_queues,
            )

            # Attach source/sink if applicable
            if isinstance(declaration, SourceDeclaration):
                if task_config.node_name in self.sources:
                    task.set_source(self.sources[task_config.node_name])

            if isinstance(declaration, OutputDeclaration):
                if task_config.node_name in self.sinks:
                    task.set_sink(self.sinks[task_config.node_name])
                task.set_max_window_size(max_window_size)

            self.tasks[task_config.task_id] = task

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        """Run all tasks concurrently."""
        # Initialize all tasks
        for task in self.tasks.values():
            task.initialize(start)

        # Run all tasks
        task_coroutines = [task.run(start, end) for task in self.tasks.values()]
        await asyncio.gather(*task_coroutines)


class SingleWorkerOrchestrator:
    """Orchestrator for single-worker mode (all partitions on one worker)."""

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

    def setup_tables_and_views(self) -> None:
        """Create database tables and views."""
        # Create tables for sources
        for node in self.user_dag.nodes:
            if node.node_type == "source":
                decl: SourceDeclaration = node.declaration  # type: ignore
                self.engine.create_table(node.name, decl.schema)

        # Create views
        for node in self.user_dag.nodes:
            if node.node_type == "view":
                decl: ViewDeclaration = node.declaration  # type: ignore
                self.engine.create_view(node.name, decl.sql)

    def compute_max_window_size(self) -> dt.timedelta:
        """Compute maximum window size across all views and outputs."""
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
        """Run with all tasks on a single worker."""
        self.setup_tables_and_views()
        max_window_size = self.compute_max_window_size()

        # Create single worker with all tasks
        all_tasks = list(self.exec_dag.tasks.values())
        worker = Worker(
            worker_id=0,
            exec_dag=self.exec_dag,
            user_dag=self.user_dag,
            engine=self.engine,
            sources=self.sources,
            sinks=self.sinks,
            task_configs=all_tasks,
        )
        worker.setup(max_window_size)

        await worker.run(start, end)
