"""Launcher for distributed cluster testing."""

import asyncio
import datetime as dt
import multiprocessing as mp
from dataclasses import dataclass
from multiprocessing import Queue
from typing import TYPE_CHECKING

from quackflow.app import Quackflow
from quackflow._internal.distributed.config import ClusterConfig, WorkerInfo, assign_tasks_to_workers
from quackflow._internal.distributed.worker import DistributedWorkerOrchestrator
from quackflow._internal.execution import ExecutionDAG

if TYPE_CHECKING:
    from quackflow.sink import Sink
    from quackflow.source import Source


def _worker_main(
    worker_info: WorkerInfo,
    cluster_config: ClusterConfig,
    sources: dict[str, "Source"],
    sinks: dict[str, "Sink"],
    start: dt.datetime,
    end: dt.datetime | None,
    result_queue: Queue,
    debug: bool = False,
) -> None:
    """Entry point for worker subprocess."""
    import logging
    import traceback

    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format=f"[{worker_info.worker_id}] %(name)s - %(message)s",
        )

    try:
        orchestrator = DistributedWorkerOrchestrator(
            worker_info=worker_info,
            cluster_config=cluster_config,
            sources=sources,
            sinks=sinks,
        )
        asyncio.run(orchestrator.run(start, end))

        # Send sink results back to main process
        sink_results = {}
        for name, sink in sinks.items():
            if hasattr(sink, "batches"):
                sink_results[name] = sink.batches  # type: ignore
        result_queue.put((worker_info.worker_id, sink_results, None))

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        result_queue.put((worker_info.worker_id, None, error_msg))


@dataclass
class DistributedCluster:
    """Handle to a running distributed cluster."""

    processes: list[mp.Process]
    cluster_config: ClusterConfig
    result_queue: Queue
    _results: dict[str, dict] | None = None

    def wait(self) -> None:
        """Wait for all worker processes to complete."""
        for p in self.processes:
            p.join()

    def collect_results(self) -> dict[str, dict]:
        """Collect sink results from all workers."""
        if self._results is not None:
            return self._results

        self._results = {}
        for _ in self.processes:
            worker_id, sink_results, error = self.result_queue.get()
            if error is not None:
                raise RuntimeError(f"Worker {worker_id} failed: {error}")
            self._results[worker_id] = sink_results

        return self._results

    def collect_sink_results(self, sink_name: str) -> list:
        """Collect results from a specific sink across all workers."""
        all_results = self.collect_results()
        batches = []
        for worker_results in all_results.values():
            if sink_name in worker_results:
                batches.extend(worker_results[sink_name])
        return batches

    def shutdown(self) -> None:
        """Terminate all worker processes."""
        for p in self.processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)


def launch_distributed_cluster(
    app: Quackflow,
    sources_by_partition: dict[int, dict[str, "Source"]],
    sinks_by_partition: dict[int, dict[str, "Sink"]],
    num_workers: int,
    start: dt.datetime,
    end: dt.datetime | None = None,
    base_port: int = 50051,
    debug: bool = False,
) -> DistributedCluster:
    """
    Launch a distributed cluster for testing.

    Args:
        app: The Quackflow application
        sources_by_partition: Mapping of partition_id -> source dict
        sinks_by_partition: Mapping of partition_id -> sink dict
        num_workers: Number of worker processes to spawn
        start: Start timestamp for execution
        end: End timestamp for execution
        base_port: Starting port for Flight servers
        debug: Enable debug logging in worker processes

    Returns:
        DistributedCluster handle for managing the cluster
    """
    ctx = mp.get_context("spawn")

    # Compile DAGs
    user_dag = app.compile()
    exec_dag = ExecutionDAG.from_user_dag(user_dag, num_workers)

    # Assign tasks to workers
    task_assignments = assign_tasks_to_workers(exec_dag, num_workers)

    # Build worker info
    workers: dict[str, WorkerInfo] = {}
    for i in range(num_workers):
        worker_id = f"worker_{i}"
        workers[worker_id] = WorkerInfo(
            worker_id=worker_id,
            host="localhost",
            port=base_port + i,
            task_ids=task_assignments[worker_id],
        )

    cluster_config = ClusterConfig(
        workers=workers,
        exec_dag=exec_dag,
        user_dag=user_dag,
    )

    # Create result queue for collecting sink data
    result_queue = ctx.Queue()

    # Spawn worker processes
    processes = []
    for i in range(num_workers):
        worker_id = f"worker_{i}"
        worker_info = workers[worker_id]

        # Get sources/sinks for this partition
        partition_sources = sources_by_partition.get(i, {})
        partition_sinks = sinks_by_partition.get(i, {})

        p = ctx.Process(
            target=_worker_main,
            args=(
                worker_info,
                cluster_config,
                partition_sources,
                partition_sinks,
                start,
                end,
                result_queue,
                debug,
            ),
        )
        p.start()
        processes.append(p)

    return DistributedCluster(
        processes=processes,
        cluster_config=cluster_config,
        result_queue=result_queue,
    )
