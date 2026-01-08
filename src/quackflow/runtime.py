import asyncio
import datetime as dt
import typing

from quackflow.app import DataPacket, Node, OutputDeclaration, Quackflow, SourceDeclaration
from quackflow.engine import Engine
from quackflow.source import ReplayableSource, Source

if typing.TYPE_CHECKING:
    from quackflow.sink import Sink


class OutputStep:
    def __init__(self, node: Node, engine: Engine, sink: "Sink"):
        self._node = node
        self._declaration: OutputDeclaration = node.declaration  # type: ignore[assignment]
        self._engine = engine
        self._sink = sink
        self._records_at_last_fire = 0
        self._last_fired_window: dt.datetime | None = None

    @property
    def effective_watermark(self) -> dt.datetime | None:
        """Effective watermark from the node (min of upstream watermarks)."""
        return self._node.effective_watermark

    def initialize(self, start: dt.datetime) -> None:
        if self._declaration.trigger_window is not None:
            self._last_fired_window = self._snap_to_window(start)

    def _snap_to_window(self, watermark: dt.datetime) -> dt.datetime:
        """Snap watermark down to the previous window boundary."""
        window_seconds = int(self._declaration.trigger_window.total_seconds())  # type: ignore[union-attr]
        midnight = watermark.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_since_midnight = (watermark - midnight).total_seconds()
        snapped_seconds = int(seconds_since_midnight // window_seconds) * window_seconds
        return midnight + dt.timedelta(seconds=snapped_seconds)

    async def on_advance(self) -> None:
        """Called when the node's effective watermark advances."""
        while self._should_fire():
            await self._fire()

    def _should_fire(self) -> bool:
        if self._declaration.trigger_records is not None:
            if self._node.total_records - self._records_at_last_fire >= self._declaration.trigger_records:
                return True

        if self._declaration.trigger_window is not None:
            watermark = self.effective_watermark
            if watermark is not None and self._last_fired_window is not None:
                current_window = self._snap_to_window(watermark)
                if current_window > self._last_fired_window:
                    return True

        return False

    async def _fire(self) -> None:
        self._records_at_last_fire = self._node.total_records
        if self._declaration.trigger_window is not None and self._last_fired_window is not None:
            window_seconds = int(self._declaration.trigger_window.total_seconds())
            self._last_fired_window = self._last_fired_window + dt.timedelta(seconds=window_seconds)
            self._engine.set_window_end(self._last_fired_window)
        result = self._engine.query(self._declaration.sql)
        await self._sink.write(result)


class ImportStep:
    def __init__(self, node: Node, source: Source, engine: Engine, source_stopped: dict[str, bool]):
        self._node = node
        self._source = source
        self._engine = engine
        self._source_stopped = source_stopped

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        source = self._source

        if isinstance(source, ReplayableSource):
            await source.seek(start)
        await source.start()

        try:
            while True:
                if end is not None:
                    watermark = source.watermark
                    if watermark is not None and watermark >= end:
                        self._source_stopped[self._node.name] = True
                        break

                batch = await source.read()
                if batch.num_rows > 0:
                    self._engine.insert(self._node.name, batch)
                    watermark = source.watermark
                    if watermark is not None:
                        packet = DataPacket(batch=batch, watermark=watermark)
                        await self._node.send(packet)

                if batch.num_rows == 0 and all(self._source_stopped.values()):
                    break
        finally:
            await source.stop()


class Runtime:
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

    async def execute(self, start: dt.datetime, end: dt.datetime | None = None) -> None:
        dag = self._app.compile()
        source_stopped: dict[str, bool] = {}

        for node in dag.source_nodes():
            declaration: SourceDeclaration = node.declaration  # type: ignore[assignment]
            self._engine.create_table(node.name, declaration.schema)
            source_stopped[node.name] = False
            node._watermark = start

        for node in dag.nodes:
            if node.node_type == "view":
                self._engine.create_view(node.name, node.declaration.sql)  # type: ignore[union-attr]

        output_steps: list[OutputStep] = []
        for node in dag.output_nodes():
            sink = self._sinks[node.name]
            step = OutputStep(node, self._engine, sink)
            step.initialize(start)
            node.set_on_advance_callback(step.on_advance)
            output_steps.append(step)

        import_steps: list[ImportStep] = []
        for node in dag.source_nodes():
            source = self._sources[node.name]
            step = ImportStep(node, source, self._engine, source_stopped)
            import_steps.append(step)

        tasks = [asyncio.create_task(step.run(start, end)) for step in import_steps]
        await asyncio.gather(*tasks)

        for output_step in output_steps:
            if output_step._should_fire():
                await output_step._fire()
