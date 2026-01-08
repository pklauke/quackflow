import datetime as dt
import typing

from quackflow.app import Quackflow, SourceDeclaration
from quackflow.engine import Engine
from quackflow.source import Source
from quackflow.window import register_window_functions_batch

if typing.TYPE_CHECKING:
    from quackflow.sink import Sink


class BatchRuntime:
    def __init__(
        self,
        app: Quackflow,
        sources: dict[str, Source],
        sinks: dict[str, "Sink"],
    ):
        self._app = app
        self._sources = sources
        self._sinks = sinks
        self._engine = Engine()
        register_window_functions_batch(self._engine._conn)

    async def execute(self, start: dt.datetime, end: dt.datetime) -> None:
        dag = self._app.compile()

        self._engine._conn.execute("SET VARIABLE __batch_start = $1::TIMESTAMP", [start])
        self._engine._conn.execute("SET VARIABLE __batch_end = $1::TIMESTAMP", [end])

        for node in dag.source_nodes():
            declaration: SourceDeclaration = node.declaration  # type: ignore[assignment]
            self._engine.create_table(node.name, declaration.schema)

        for name, source in self._sources.items():
            await source.start()
            while True:
                batch = await source.read()
                if batch.num_rows == 0:
                    break
                self._engine.insert(name, batch)
            await source.stop()

        for node in dag.nodes:
            if node.node_type == "view":
                self._engine.create_view(node.name, node.declaration.sql)  # type: ignore[union-attr]

        for node in dag.output_nodes():
            result = self._engine.query(node.declaration.sql)  # type: ignore[union-attr]
            sink = self._sinks[node.name]
            await sink.write(result)
