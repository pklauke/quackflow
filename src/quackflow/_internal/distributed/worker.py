"""Distributed worker orchestrator."""

import asyncio
import datetime as dt
import threading
from typing import TYPE_CHECKING, Any

import pyarrow.flight as flight

from quackflow.app import SourceDeclaration, ViewDeclaration
from quackflow._internal.distributed.config import ClusterConfig, WorkerInfo
from quackflow._internal.distributed.flight_client import FlightClientPool
from quackflow._internal.distributed.flight_server import QuackflowFlightServer
from quackflow._internal.engine import Engine
from quackflow._internal.execution import TaskConfig
from quackflow._internal.transport import (
    DownstreamHandle,
    LocalDownstreamHandle,
    LocalUpstreamHandle,
    RemoteDownstreamHandle,
    RemoteUpstreamHandle,
    UpstreamHandle,
)
from quackflow._internal.worker import BaseOrchestrator

if TYPE_CHECKING:
    from quackflow.sink import Sink
    from quackflow.source import Source


class DistributedWorkerOrchestrator(BaseOrchestrator):
    """Orchestrator for a single worker in a distributed cluster."""

    def __init__(
        self,
        worker_info: WorkerInfo,
        cluster_config: ClusterConfig,
        sources: dict[str, "Source"],
        sinks: dict[str, "Sink"],
    ):
        super().__init__(
            exec_dag=cluster_config.exec_dag,
            user_dag=cluster_config.user_dag,
            sources=sources,
            sinks=sinks,
        )
        self.worker_info = worker_info
        self.cluster_config = cluster_config
        self.flight_server: QuackflowFlightServer | None = None
        self.client_pool = FlightClientPool()
        self._loop: asyncio.AbstractEventLoop | None = None

    def _get_task_ids(self) -> list[str]:
        return list(self.worker_info.task_ids)

    def _get_engine(self, task_config: TaskConfig) -> Engine:
        return self._create_engine_for_task(task_config)

    def _extra_task_kwargs(self) -> dict[str, Any]:
        return {
            "num_partitions": self.exec_dag.num_partitions,
            "propagate_batch": True,
        }

    async def _before_execute(self) -> None:
        self._loop = asyncio.get_event_loop()

        location = f"grpc://0.0.0.0:{self.worker_info.port}"
        self.flight_server = QuackflowFlightServer(
            location=location,
            tasks=self.tasks,
            loop=self._loop,
        )

        server_thread = threading.Thread(target=self.flight_server.serve, daemon=True)
        server_thread.start()

        await self._wait_for_cluster_ready()

    async def _cleanup(self) -> None:
        self.client_pool.close()
        if self.flight_server is not None:
            self.flight_server.shutdown()

    def _create_downstream_handle(self, sender_id: str, target_id: str) -> DownstreamHandle:
        if target_id in self.tasks:
            return LocalDownstreamHandle(sender_id, self.tasks[target_id])

        worker = self.cluster_config.get_worker_for_task(target_id)
        if worker is None:
            raise ValueError(f"No worker found for task {target_id}")
        target_config = self.exec_dag.get_task(target_id)
        return RemoteDownstreamHandle(
            sender_id=sender_id,
            target_task_id=target_id,
            target_repartition_key=target_config.repartition_key,
            target_host=worker.host,
            target_port=worker.port,
            client_pool=self.client_pool,
        )

    def _create_upstream_handle(self, sender_id: str, target_id: str) -> UpstreamHandle:
        if target_id in self.tasks:
            return LocalUpstreamHandle(sender_id, self.tasks[target_id])

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

    def _create_engine_for_task(self, task_config: TaskConfig) -> Engine:
        """Create a dedicated engine for a task with required tables/views."""
        engine = Engine()
        upstream_node_names = {task_id.split("[")[0] for task_id in task_config.upstream_tasks}

        for node in self.user_dag.nodes:
            decl = node.declaration
            if isinstance(decl, SourceDeclaration):
                engine.create_table(node.name, decl.schema)
            elif isinstance(decl, ViewDeclaration):
                if node.name not in upstream_node_names:
                    engine.create_view(node.name, decl.sql)

        return engine

    async def _wait_for_cluster_ready(self) -> None:
        """Wait until all workers are accepting connections."""
        for worker in self.cluster_config.workers.values():
            if worker.worker_id == self.worker_info.worker_id:
                continue

            while True:
                try:
                    client = flight.connect(f"grpc://{worker.host}:{worker.port}")
                    client.close()
                    break
                except Exception:
                    await asyncio.sleep(0.1)
