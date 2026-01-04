import datetime as dt

from quackflow.app import DAG, OutputConfig, Quackflow
from quackflow.engine import Engine
from quackflow.source import ReplayableSource


class OutputState:
    def __init__(self, config: OutputConfig):
        self.config = config
        self.records_since_last_fire = 0
        self.watermark: dt.datetime | None = None


class Runtime:
    def __init__(self, app: Quackflow):
        self._app = app
        self._dag: DAG | None = None
        self._engine = Engine()
        self._output_states: list[OutputState] = []
        self._source_records: dict[str, int] = {}
        self._source_stopped: dict[str, bool] = {}

    async def execute(self, start: dt.datetime, end: dt.datetime | None = None) -> None:
        self._dag = self._app.compile()

        for name, source_config in self._app.sources.items():
            self._engine.create_table(name, source_config.schema)
            self._source_records[name] = 0
            self._source_stopped[name] = False

        for name, view_config in self._app.views.items():
            self._engine.create_view(name, view_config.sql)

        self._output_states = [OutputState(config) for config in self._app.outputs]
        for state in self._output_states:
            state.watermark = start

        for source_config in self._app.sources.values():
            if isinstance(source_config.source, ReplayableSource):
                await source_config.source.seek(start)
            await source_config.source.start()

        try:
            while True:
                any_progress = await self._tick(end)

                if end is not None and self._all_outputs_reached(end):
                    break

                if not any_progress and all(self._source_stopped.values()):
                    break
        finally:
            for source_config in self._app.sources.values():
                await source_config.source.stop()

    async def _tick(self, end: dt.datetime | None) -> bool:
        any_progress = False

        for name, source_config in self._app.sources.items():
            if self._source_stopped[name]:
                continue

            if end is not None:
                watermark = source_config.source.watermark
                if watermark is not None and watermark >= end:
                    self._source_stopped[name] = True
                    continue

            batch = await source_config.source.read()
            if batch.num_rows > 0:
                self._engine.insert(name, batch)
                self._source_records[name] += batch.num_rows
                any_progress = True

                new_watermark = source_config.time_notion.compute_watermark(batch)
                for state in self._output_states:
                    if state.watermark is None or new_watermark > state.watermark:
                        state.watermark = new_watermark

        for state in self._output_states:
            if self._should_fire(state):
                await self._fire_output(state)

        return any_progress

    def _should_fire(self, state: OutputState) -> bool:
        config = state.config

        if config.trigger_records is not None:
            total = sum(self._source_records.values())
            if total - state.records_since_last_fire >= config.trigger_records:
                return True

        return False

    async def _fire_output(self, state: OutputState) -> None:
        state.records_since_last_fire = sum(self._source_records.values())
        result = self._engine.query(state.config.sql)
        await state.config.sink.write(result)

    def _all_outputs_reached(self, end: dt.datetime) -> bool:
        return all(state.watermark is not None and state.watermark >= end for state in self._output_states)
