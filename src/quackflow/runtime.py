"""Runtime for executing quackflow pipelines."""

import datetime as dt
import typing

from quackflow.app import Quackflow
from quackflow._internal.engine import Engine
from quackflow._internal.execution import ExecutionDAG
from quackflow.source import Source

if typing.TYPE_CHECKING:
    from quackflow.sink import Sink


class Runtime:
    """Runtime with support for multiple partitions and repartitioning."""

    def __init__(
        self,
        app: Quackflow,
        sources: dict[str, Source],
        sinks: dict[str, "Sink"],
        num_partitions: int = 1,
    ):
        self._app = app
        self._sources = sources
        self._sinks = sinks
        self._num_partitions = num_partitions
        self._engine = Engine()

    async def execute(self, start: dt.datetime, end: dt.datetime | None = None) -> None:
        """Execute the pipeline with parallel tasks."""
        from quackflow._internal.worker import SingleWorkerOrchestrator

        # Compile user DAG and build execution DAG
        user_dag = self._app.compile()
        exec_dag = ExecutionDAG.from_user_dag(user_dag, self._num_partitions)

        # Use single worker orchestrator (all partitions on one worker)
        orchestrator = SingleWorkerOrchestrator(
            exec_dag=exec_dag,
            user_dag=user_dag,
            engine=self._engine,
            sources=self._sources,
            sinks=self._sinks,
        )

        await orchestrator.run(start, end)
