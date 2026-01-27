"""Distributed worker orchestrator."""

import asyncio
import datetime as dt
import threading
from typing import TYPE_CHECKING

import pyarrow.flight as flight

from quackflow.app import SourceDeclaration, ViewDeclaration
from quackflow._internal.distributed.config import ClusterConfig, WorkerInfo
from quackflow._internal.distributed.flight_client import FlightClientPool
from quackflow._internal.distributed.flight_server import QuackflowFlightServer
from quackflow._internal.engine import Engine
from quackflow._internal.task import Task
from quackflow._internal.transport import (
    LocalDownstreamHandle,
    LocalUpstreamHandle,
    RemoteDownstreamHandle,
    RemoteUpstreamHandle,
)
from quackflow._internal.worker_utils import (
    compute_max_window_size,
    compute_max_window_size_per_source,
    create_task,
)

if TYPE_CHECKING:
    from quackflow.sink import Sink
    from quackflow.source import Source


class DistributedWorkerOrchestrator:
    """Orchestrator for a single worker in a distributed cluster."""

    def __init__(
        self,
        worker_info: WorkerInfo,
        cluster_config: ClusterConfig,
        sources: dict[str, "Source"],
        sinks: dict[str, "Sink"],
    ):
        self.worker_info = worker_info
        self.cluster_config = cluster_config
        self.sources = sources
        self.sinks = sinks

        self.tasks: dict[str, Task] = {}
        self.flight_server: QuackflowFlightServer | None = None
        self.client_pool = FlightClientPool()
        self._loop: asyncio.AbstractEventLoop | None = None

    def _create_engine_for_task(self, task_config) -> Engine:
        """Create a dedicated engine for a task with required tables/views."""
        user_dag = self.cluster_config.user_dag
        engine = Engine()

        # Create tables and views needed by this task
        for node in user_dag.nodes:
            if node.node_type == "source":
                decl: SourceDeclaration = node.declaration  # type: ignore
                engine.create_table(node.name, decl.schema)
            elif node.node_type == "view":
                decl: ViewDeclaration = node.declaration  # type: ignore
                engine.create_view(node.name, decl.sql)

        return engine

    def setup(self) -> None:
        """Initialize tables, views, tasks, and wiring."""
        user_dag = self.cluster_config.user_dag
        exec_dag = self.cluster_config.exec_dag
        max_window_size = compute_max_window_size(user_dag)
        source_window_sizes = compute_max_window_size_per_source(user_dag)

        # Create only tasks assigned to this worker, each with its own engine
        for task_id in self.worker_info.task_ids:
            task_config = exec_dag.get_task(task_id)
            engine = self._create_engine_for_task(task_config)
            task = create_task(
                task_config,
                user_dag,
                engine,
                self.sources,
                self.sinks,
                max_window_size,
                source_window_sizes,
                num_partitions=exec_dag.num_partitions,
                propagate_batch=True,
            )
            self.tasks[task_id] = task

        # Wire up handles - local or remote based on task location
        for task_id in self.worker_info.task_ids:
            task_config = exec_dag.get_task(task_id)
            task = self.tasks[task_id]

            for downstream_id in task_config.downstream_tasks:
                handle = self._create_downstream_handle(task_id, downstream_id)
                task.downstream_handles.append(handle)

            for upstream_id in task_config.upstream_tasks:
                handle = self._create_upstream_handle(task_id, upstream_id)
                task.upstream_handles.append(handle)

    def _create_downstream_handle(self, sender_id: str, target_id: str):
        """Create local or remote downstream handle based on task location."""
        if target_id in self.tasks:
            # Same worker - direct call
            return LocalDownstreamHandle(sender_id, self.tasks[target_id])
        else:
            # Different worker - Arrow Flight
            worker = self.cluster_config.get_worker_for_task(target_id)
            if worker is None:
                raise ValueError(f"No worker found for task {target_id}")
            target_config = self.cluster_config.exec_dag.get_task(target_id)
            return RemoteDownstreamHandle(
                sender_id=sender_id,
                target_task_id=target_id,
                target_repartition_key=target_config.repartition_key,
                target_host=worker.host,
                target_port=worker.port,
                client_pool=self.client_pool,
            )

    def _create_upstream_handle(self, sender_id: str, target_id: str):
        """Create local or remote upstream handle based on task location."""
        if target_id in self.tasks:
            return LocalUpstreamHandle(sender_id, self.tasks[target_id])
        else:
            worker = self.cluster_config.get_worker_for_task(target_id)
            if worker is None:
                raise ValueError(f"No worker found for task {target_id}")
            return RemoteUpstreamHandle(
                sender_id=sender_id,
                target_task_id=target_id,
                target_host=worker.host,
                target_port=worker.port,
                client_pool=self.client_pool,
            )

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        """Run the worker."""
        self._loop = asyncio.get_event_loop()
        self.setup()

        # Start Flight server in background thread
        location = f"grpc://0.0.0.0:{self.worker_info.port}"
        self.flight_server = QuackflowFlightServer(
            location=location,
            tasks=self.tasks,
            loop=self._loop,
        )

        server_thread = threading.Thread(target=self.flight_server.serve, daemon=True)
        server_thread.start()

        # Wait for all workers to be ready
        await self._wait_for_cluster_ready()

        # Initialize tasks
        for task in self.tasks.values():
            task.initialize(start)

        source_tasks = [
            task.run_source(start, end) for task in self.tasks.values() if task.config.node_type == "source"
        ]
        await asyncio.gather(*source_tasks)

        # Final fire for output tasks
        for task in self.tasks.values():
            await task.final_fire()

        # Cleanup
        self.client_pool.close()
        self.flight_server.shutdown()

    async def _wait_for_cluster_ready(self) -> None:
        """Wait until all workers are accepting connections."""
        for worker in self.cluster_config.workers.values():
            if worker.worker_id == self.worker_info.worker_id:
                continue  # Skip self

            while True:
                try:
                    client = flight.connect(f"grpc://{worker.host}:{worker.port}")
                    client.close()
                    break
                except Exception:
                    await asyncio.sleep(0.1)
