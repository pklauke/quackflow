import asyncio
import datetime as dt
import typing

import pyarrow as pa

from quackflow.app import DataPacket, Node, OutputDeclaration, Quackflow, SourceDeclaration
from quackflow.engine import Engine
from quackflow.source import ReplayableSource, Source

if typing.TYPE_CHECKING:
    from quackflow.sink import Sink


class OutputStep:
    def __init__(self, node: Node, engine: Engine, sink: "Sink", max_window_size: dt.timedelta):
        self._node = node
        self._declaration: OutputDeclaration = node.declaration  # type: ignore[assignment]
        self._engine = engine
        self._sink = sink
        self._max_window_size = max_window_size
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
        if self._should_fire():
            target = None
            if self._declaration.trigger_window is not None and self.effective_watermark is not None:
                target = self._snap_to_window(self.effective_watermark)
            await self._fire(target)

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

    async def _fire(self, fire_up_to: dt.datetime | None = None) -> None:
        self._records_at_last_fire = self._node.total_records
        if self._declaration.trigger_window is not None and self._last_fired_window is not None:
            batch_start = self._last_fired_window - self._max_window_size + self._declaration.trigger_window
            batch_end = (
                fire_up_to if fire_up_to is not None else self._last_fired_window + self._declaration.trigger_window
            )
            self._last_fired_window = batch_end
            self._engine.set_batch_start(batch_start)
            self._engine.set_batch_end(batch_end)
            self._engine.set_window_hop(self._declaration.trigger_window)
        result = self._engine.query(self._declaration.sql)
        await self._emit_by_window(result)
        await self._propagate_expiration()

    async def _emit_by_window(self, result: pa.RecordBatch) -> None:
        """Emit results grouped by window_end in order."""
        if "window_end" not in result.schema.names or result.num_rows == 0:
            await self._sink.write(result)
            return

        table = pa.Table.from_batches([result])
        table = table.sort_by("window_end")
        window_ends = table.column("window_end")

        current_end = None
        start_idx = 0
        for i, end in enumerate(window_ends):
            if current_end is None:
                current_end = end
            elif end != current_end:
                batch = table.slice(start_idx, i - start_idx).to_batches()[0]
                await self._sink.write(batch)
                current_end = end
                start_idx = i

        if start_idx < len(window_ends):
            batch = table.slice(start_idx).to_batches()[0]
            await self._sink.write(batch)

    async def _propagate_expiration(self) -> None:
        if self._last_fired_window is None:
            return

        max_window = max(self._declaration.window_sizes, default=dt.timedelta(0))
        threshold = self._last_fired_window - max_window

        for upstream in self._node.upstream:
            await upstream.receive_expiration_threshold(self._node.name, threshold)


class ImportStep:
    def __init__(self, node: Node, source: Source, engine: Engine, source_stopped: dict[str, bool]):
        self._node = node
        self._declaration: SourceDeclaration = node.declaration  # type: ignore[assignment]
        self._source = source
        self._engine = engine
        self._source_stopped = source_stopped

    async def on_expiration(self) -> None:
        threshold = self._node.expiration_threshold
        ts_col = self._declaration.ts_col
        if threshold is not None and ts_col is not None:
            self._engine.delete_before(self._node.name, ts_col, threshold)

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

        all_window_sizes: list[dt.timedelta] = []
        for view in self._app.views.values():
            all_window_sizes.extend(view.window_sizes)
        for output in self._app.outputs.values():
            all_window_sizes.extend(output.window_sizes)
        max_window_size = max(all_window_sizes, default=dt.timedelta(0))

        output_steps: list[OutputStep] = []
        for node in dag.output_nodes():
            sink = self._sinks[node.name]
            decl: OutputDeclaration = node.declaration  # type: ignore[assignment]
            effective_max = max(max_window_size, decl.trigger_window or dt.timedelta(0))
            step = OutputStep(node, self._engine, sink, effective_max)
            step.initialize(start)
            node.set_on_advance_callback(step.on_advance)
            output_steps.append(step)

        import_steps: list[ImportStep] = []
        for node in dag.source_nodes():
            source = self._sources[node.name]
            step = ImportStep(node, source, self._engine, source_stopped)
            node.set_on_expiration_callback(step.on_expiration)
            import_steps.append(step)

        tasks = [asyncio.create_task(step.run(start, end)) for step in import_steps]
        await asyncio.gather(*tasks)

        for output_step in output_steps:
            if output_step._should_fire():
                target = None
                decl = output_step._declaration
                if decl.trigger_window is not None and output_step.effective_watermark is not None:
                    target = output_step._snap_to_window(output_step.effective_watermark)
                await output_step._fire(target)
