import asyncio
import datetime as dt

from quackflow.app import Node, Quackflow, SourceBinding
from quackflow.engine import Engine
from quackflow.source import ReplayableSource


class RuntimeState:
    def __init__(self):
        self.source_records: dict[str, int] = {}
        self.source_stopped: dict[str, bool] = {}


class OutputStep:
    def __init__(self, node: Node, engine: Engine, state: RuntimeState):
        self._node = node
        self._engine = engine
        self._state = state
        self._records_at_last_fire = 0
        self._last_fired_window: dt.datetime | None = None

    @property
    def effective_watermark(self) -> dt.datetime | None:
        """Effective watermark from the node (min of upstream watermarks)."""
        return self._node.effective_watermark

    def initialize(self, start: dt.datetime) -> None:
        if self._node.binding.trigger_window is not None:
            self._last_fired_window = self._snap_to_window(start)

    def _snap_to_window(self, watermark: dt.datetime) -> dt.datetime:
        """Snap watermark down to the previous window boundary."""
        window_seconds = int(self._node.binding.trigger_window.total_seconds())  # type: ignore[union-attr]
        midnight = watermark.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_since_midnight = (watermark - midnight).total_seconds()
        snapped_seconds = int(seconds_since_midnight // window_seconds) * window_seconds
        return midnight + dt.timedelta(seconds=snapped_seconds)

    async def on_watermark_advance(self) -> None:
        """Called when an upstream source's watermark advances."""
        while self._should_fire():
            await self._fire()

    def _should_fire(self) -> bool:
        if self._node.binding.trigger_records is not None:
            total = sum(self._state.source_records.values())
            if total - self._records_at_last_fire >= self._node.binding.trigger_records:
                return True

        if self._node.binding.trigger_window is not None:
            watermark = self.effective_watermark
            if watermark is not None and self._last_fired_window is not None:
                current_window = self._snap_to_window(watermark)
                if current_window > self._last_fired_window:
                    return True

        return False

    async def _fire(self) -> None:
        self._records_at_last_fire = sum(self._state.source_records.values())
        if self._node.binding.trigger_window is not None and self._last_fired_window is not None:
            window_seconds = int(self._node.binding.trigger_window.total_seconds())
            self._last_fired_window = self._last_fired_window + dt.timedelta(seconds=window_seconds)
            self._engine.set_window_end(self._last_fired_window)
        elif self.effective_watermark is not None:
            self._engine.set_window_end(self.effective_watermark)
        result = self._engine.query(self._node.binding.sql)
        await self._node.binding.sink.write(result)


class ImportStep:
    def __init__(self, node: Node, engine: Engine, state: RuntimeState):
        self._node = node
        self._binding: SourceBinding = node.binding  # type: ignore[assignment]
        self._engine = engine
        self._state = state

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        source = self._binding.source

        if isinstance(source, ReplayableSource):
            await source.seek(start)
        await source.start()

        try:
            while True:
                if end is not None:
                    watermark = source.watermark
                    if watermark is not None and watermark >= end:
                        self._state.source_stopped[self._node.name] = True
                        break

                batch = await source.read()
                if batch.num_rows > 0:
                    self._engine.insert(self._node.name, batch)
                    self._state.source_records[self._node.name] = (
                        self._state.source_records.get(self._node.name, 0) + batch.num_rows
                    )

                    new_watermark = source.watermark
                    if new_watermark is not None:
                        await self._node.set_watermark(new_watermark)

                if batch.num_rows == 0 and all(self._state.source_stopped.values()):
                    break
        finally:
            await source.stop()


class Runtime:
    def __init__(self, app: Quackflow):
        self._app = app
        self._engine = Engine()
        self._state = RuntimeState()

    async def execute(self, start: dt.datetime, end: dt.datetime | None = None) -> None:
        dag = self._app.compile()

        for node in dag.source_nodes():
            binding: SourceBinding = node.binding  # type: ignore[assignment]
            self._engine.create_table(node.name, binding.schema)
            self._state.source_records[node.name] = 0
            self._state.source_stopped[node.name] = False
            node._watermark = start

        for node in dag.nodes:
            if node.node_type == "view":
                binding = node.binding
                self._engine.create_view(node.name, binding.sql)  # type: ignore[union-attr]

        output_steps: list[OutputStep] = []
        for node in dag.output_nodes():
            step = OutputStep(node, self._engine, self._state)
            step.initialize(start)
            node.set_watermark_callback(step.on_watermark_advance)
            output_steps.append(step)

        import_steps: list[ImportStep] = []
        for node in dag.source_nodes():
            step = ImportStep(node, self._engine, self._state)
            import_steps.append(step)

        tasks = [asyncio.create_task(step.run(start, end)) for step in import_steps]
        await asyncio.gather(*tasks)

        for output_step in output_steps:
            if output_step._should_fire():
                await output_step._fire()
