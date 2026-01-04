import asyncio
import datetime as dt

from quackflow.app import OutputConfig, Quackflow, SourceConfig
from quackflow.engine import Engine
from quackflow.source import ReplayableSource


class RuntimeState:
    def __init__(self):
        self.source_records: dict[str, int] = {}
        self.source_stopped: dict[str, bool] = {}


class OutputStep:
    def __init__(
        self,
        config: OutputConfig,
        engine: Engine,
        state: RuntimeState,
        upstream_sources: list[str],
    ):
        self._config = config
        self._engine = engine
        self._state = state
        self._upstream_watermarks: dict[str, dt.datetime | None] = dict.fromkeys(upstream_sources, None)
        self._records_at_last_fire = 0
        self._last_fired_window: dt.datetime | None = None

    @property
    def watermark(self) -> dt.datetime | None:
        watermarks = [w for w in self._upstream_watermarks.values() if w is not None]
        return min(watermarks) if watermarks else None

    def initialize_watermarks(self, start: dt.datetime) -> None:
        for name in self._upstream_watermarks:
            self._upstream_watermarks[name] = start
        if self._config.trigger_window is not None:
            self._last_fired_window = self._snap_to_window(start)

    def _snap_to_window(self, watermark: dt.datetime) -> dt.datetime:
        """Snap watermark down to the previous window boundary."""
        window_seconds = int(self._config.trigger_window.total_seconds())  # type: ignore[union-attr]
        midnight = watermark.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_since_midnight = (watermark - midnight).total_seconds()
        snapped_seconds = int(seconds_since_midnight // window_seconds) * window_seconds
        return midnight + dt.timedelta(seconds=snapped_seconds)

    async def receive_watermark(self, source_name: str, watermark: dt.datetime) -> None:
        old_effective = self.watermark
        self._upstream_watermarks[source_name] = watermark
        new_effective = self.watermark

        if new_effective is not None and (old_effective is None or new_effective > old_effective):
            while self._should_fire():
                await self._fire()

    def _should_fire(self) -> bool:
        if self._config.trigger_records is not None:
            total = sum(self._state.source_records.values())
            if total - self._records_at_last_fire >= self._config.trigger_records:
                return True

        if self._config.trigger_window is not None:
            watermark = self.watermark
            if watermark is not None and self._last_fired_window is not None:
                current_window = self._snap_to_window(watermark)
                if current_window > self._last_fired_window:
                    return True

        return False

    async def _fire(self) -> None:
        self._records_at_last_fire = sum(self._state.source_records.values())
        if self._config.trigger_window is not None and self._last_fired_window is not None:
            window_seconds = int(self._config.trigger_window.total_seconds())
            self._last_fired_window = self._last_fired_window + dt.timedelta(seconds=window_seconds)
        result = self._engine.query(self._config.sql)
        await self._config.sink.write(result)


class ImportStep:
    def __init__(
        self,
        name: str,
        config: SourceConfig,
        engine: Engine,
        state: RuntimeState,
        downstream: list[OutputStep],
    ):
        self._name = name
        self._config = config
        self._engine = engine
        self._state = state
        self._downstream = downstream

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        source = self._config.source

        if isinstance(source, ReplayableSource):
            await source.seek(start)
        await source.start()

        try:
            while True:
                if end is not None:
                    watermark = source.watermark
                    if watermark is not None and watermark >= end:
                        self._state.source_stopped[self._name] = True
                        break

                batch = await source.read()
                if batch.num_rows > 0:
                    self._engine.insert(self._name, batch)
                    self._state.source_records[self._name] = (
                        self._state.source_records.get(self._name, 0) + batch.num_rows
                    )

                    new_watermark = source.watermark
                    if new_watermark is not None:
                        for output in self._downstream:
                            await output.receive_watermark(self._name, new_watermark)

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
        self._app.compile()

        for name, source_config in self._app.sources.items():
            self._engine.create_table(name, source_config.schema)
            self._state.source_records[name] = 0
            self._state.source_stopped[name] = False

        for name, view_config in self._app.views.items():
            self._engine.create_view(name, view_config.sql)

        output_steps = []
        for config in self._app.outputs:
            upstream_sources = self._find_upstream_sources(config)
            step = OutputStep(config, self._engine, self._state, upstream_sources)
            step.initialize_watermarks(start)
            output_steps.append(step)

        import_steps = []
        for name, source_config in self._app.sources.items():
            downstream = self._find_downstream_outputs(name, output_steps)
            step = ImportStep(name, source_config, self._engine, self._state, downstream)
            import_steps.append(step)

        tasks = [asyncio.create_task(step.run(start, end)) for step in import_steps]
        await asyncio.gather(*tasks)

        for output in output_steps:
            if output._should_fire():
                await output._fire()

    def _find_upstream_sources(self, output_config: OutputConfig) -> list[str]:
        result = []
        visited = set()
        to_check = list(output_config.depends_on)

        while to_check:
            name = to_check.pop()
            if name in visited:
                continue
            visited.add(name)

            if name in self._app.sources:
                result.append(name)
            elif name in self._app.views:
                to_check.extend(self._app.views[name].depends_on)

        return result

    def _find_downstream_outputs(
        self, source_name: str, output_steps: list[OutputStep]
    ) -> list[OutputStep]:
        result = []
        for output in output_steps:
            if source_name in output._upstream_watermarks:
                result.append(output)
        return result
