"""Configuration dataclasses for distributed execution."""

from dataclasses import dataclass, field

from quackflow.app import DAG
from quackflow.execution import ExecutionDAG


@dataclass
class WorkerInfo:
    """Information about a single worker in the cluster."""

    worker_id: str
    host: str
    port: int
    task_ids: list[str] = field(default_factory=list)


@dataclass
class ClusterConfig:
    """Configuration for the entire distributed cluster."""

    workers: dict[str, WorkerInfo]  # worker_id -> WorkerInfo
    exec_dag: ExecutionDAG
    user_dag: DAG
    _task_to_worker: dict[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Build task -> worker lookup."""
        for worker_id, worker in self.workers.items():
            for task_id in worker.task_ids:
                self._task_to_worker[task_id] = worker_id

    def get_worker_for_task(self, task_id: str) -> WorkerInfo | None:
        """Find which worker owns a given task."""
        worker_id = self._task_to_worker.get(task_id)
        if worker_id:
            return self.workers.get(worker_id)
        return None


def assign_tasks_to_workers(
    exec_dag: ExecutionDAG,
    num_workers: int,
) -> dict[str, list[str]]:
    """
    Assign tasks to workers based on partition ID.

    Tasks with the same partition_id go to the same worker.
    Worker index = partition_id % num_workers.
    """
    assignments: dict[str, list[str]] = {f"worker_{i}": [] for i in range(num_workers)}

    for task_id, task_config in exec_dag.tasks.items():
        worker_idx = task_config.partition_id % num_workers
        worker_id = f"worker_{worker_idx}"
        assignments[worker_id].append(task_id)

    return assignments
