"""Execution DAG - physical representation of the user DAG with parallelization."""

from dataclasses import dataclass, field

from quackflow.app import DAG, OutputDeclaration, SourceDeclaration, ViewDeclaration


@dataclass
class TaskConfig:
    """Configuration for a single execution task."""

    task_id: str
    node_name: str  # Reference to user DAG node
    node_type: str  # "source", "view", or "output"
    partition_id: int  # Which partition this task handles
    upstream_tasks: list[str] = field(default_factory=list)
    downstream_tasks: list[str] = field(default_factory=list)
    repartition_key: list[str] | None = None  # Key to repartition output by


@dataclass
class ExecutionDAG:
    """Physical DAG with tasks and repartitioning."""

    tasks: dict[str, TaskConfig] = field(default_factory=dict)
    num_partitions: int = 1

    def add_task(self, task: TaskConfig) -> None:
        self.tasks[task.task_id] = task

    def get_task(self, task_id: str) -> TaskConfig:
        return self.tasks[task_id]

    def connect(self, upstream_id: str, downstream_id: str) -> None:
        self.tasks[upstream_id].downstream_tasks.append(downstream_id)
        self.tasks[downstream_id].upstream_tasks.append(upstream_id)

    def source_tasks(self) -> list[TaskConfig]:
        return [t for t in self.tasks.values() if t.node_type == "source"]

    def output_tasks(self) -> list[TaskConfig]:
        return [t for t in self.tasks.values() if t.node_type == "output"]

    @classmethod
    def from_user_dag(cls, dag: DAG, num_partitions: int) -> "ExecutionDAG":
        """Convert user DAG to execution DAG with parallelization."""
        exec_dag = cls(num_partitions=num_partitions)

        # Create tasks for each node × partition
        for node in dag.nodes:
            for partition_id in range(num_partitions):
                task_id = f"{node.name}[{partition_id}]"
                task = TaskConfig(
                    task_id=task_id,
                    node_name=node.name,
                    node_type=node.node_type,
                    partition_id=partition_id,
                )

                # Determine repartition key from declaration
                decl = node.declaration
                if isinstance(decl, (ViewDeclaration, OutputDeclaration)):
                    task.repartition_key = decl.partition_by

                exec_dag.add_task(task)

        # Connect tasks based on user DAG edges
        for node in dag.nodes:
            for upstream in node.upstream:
                # Check if we need repartitioning between these nodes
                needs_repartition = _needs_repartition(upstream, node)

                for partition_id in range(num_partitions):
                    downstream_task_id = f"{node.name}[{partition_id}]"

                    if needs_repartition:
                        # When repartitioning, all upstream partitions connect to all downstream partitions
                        for upstream_partition_id in range(num_partitions):
                            upstream_task_id = f"{upstream.name}[{upstream_partition_id}]"
                            exec_dag.connect(upstream_task_id, downstream_task_id)
                    else:
                        # Same partition connects to same partition
                        upstream_task_id = f"{upstream.name}[{partition_id}]"
                        exec_dag.connect(upstream_task_id, downstream_task_id)

        return exec_dag


def _needs_repartition(upstream_node, downstream_node) -> bool:
    """Determine if repartitioning is needed between two nodes."""
    upstream_decl = upstream_node.declaration
    downstream_decl = downstream_node.declaration

    # Get partition keys
    upstream_key = None
    downstream_key = None

    if isinstance(upstream_decl, SourceDeclaration):
        upstream_key = upstream_decl.partition_by
    elif isinstance(upstream_decl, (ViewDeclaration, OutputDeclaration)):
        upstream_key = upstream_decl.partition_by

    if isinstance(downstream_decl, (ViewDeclaration, OutputDeclaration)):
        downstream_key = downstream_decl.partition_by

    # Repartition if downstream has an explicit partition key that differs from upstream
    if downstream_key is not None:
        if upstream_key is None or set(downstream_key) != set(upstream_key):
            return True

    return False
