"""Task - logical execution unit that runs one node's SQL for specific partition(s)."""

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from quackflow.app import OutputDeclaration, SourceDeclaration, ViewDeclaration
from quackflow.execution import TaskConfig
from quackflow.transport import (
    DownstreamHandle,
    ExpirationMessage,
    UpstreamHandle,
    WatermarkMessage,
)

logger = logging.getLogger(__name__)


def _fmt_wm(watermark: dt.datetime) -> str:
    return watermark.strftime("%H:%M:%S")


if TYPE_CHECKING:
    from quackflow.engine import Engine
    from quackflow.sink import Sink
    from quackflow.source import ReplayableSource, Source


@dataclass
class TaskState:
    upstream_watermarks: dict[str, dt.datetime] = field(default_factory=dict)
    upstream_records: dict[str, int] = field(default_factory=dict)
    last_fired_window: dt.datetime | None = None
    records_at_last_fire: int = 0


class Task:
    def __init__(
        self,
        config: TaskConfig,
        declaration: SourceDeclaration | ViewDeclaration | OutputDeclaration,
        engine: "Engine",
    ):
        self.config = config
        self.declaration = declaration
        self.engine = engine
        self._state = TaskState()

        self.downstream_handles: list[DownstreamHandle] = []
        self.upstream_handles: list[UpstreamHandle] = []

        self._source: "Source | None" = None
        self._sink: "Sink | None" = None
        self._max_window_size: dt.timedelta = dt.timedelta(0)
        self._downstream_thresholds: dict[str, dt.datetime] = {}

    def set_source(self, source: "Source") -> None:
        self._source = source

    def set_sink(self, sink: "Sink") -> None:
        self._sink = sink

    def set_max_window_size(self, size: dt.timedelta) -> None:
        self._max_window_size = size

    @property
    def effective_watermark(self) -> dt.datetime | None:
        if not self._state.upstream_watermarks:
            return None
        if len(self._state.upstream_watermarks) < len(self.upstream_handles):
            return None
        return min(self._state.upstream_watermarks.values())

    @property
    def total_records(self) -> int:
        return sum(self._state.upstream_records.values())

    @property
    def expiration_threshold(self) -> dt.datetime | None:
        if not self._downstream_thresholds:
            return None
        if len(self._downstream_thresholds) < len(self.downstream_handles):
            return None
        return min(self._downstream_thresholds.values())

    def initialize(self, start: dt.datetime) -> None:
        if isinstance(self.declaration, OutputDeclaration):
            if self.declaration.trigger_window is not None:
                self._state.last_fired_window = self._snap_to_window(start)

    async def run_source(self, start: dt.datetime, end: dt.datetime | None) -> None:
        from quackflow.source import ReplayableSource

        source = self._source
        assert source is not None

        if isinstance(source, ReplayableSource):
            await source.seek(start)
        await source.start()

        try:
            while True:
                if end is not None:
                    watermark = source.watermark
                    if watermark is not None and watermark >= end:
                        break

                batch = await source.read()
                if batch.num_rows > 0:
                    self.engine.insert(self.config.node_name, batch)
                    watermark = source.watermark
                    if watermark is not None:
                        message = WatermarkMessage(watermark=watermark, num_rows=batch.num_rows)
                        for handle in self.downstream_handles:
                            await handle.send(message)

                if batch.num_rows == 0:
                    await asyncio.sleep(0.01)
                    watermark = source.watermark
                    if end is not None and watermark is not None and watermark >= end:
                        break
        finally:
            await source.stop()

    async def receive_watermark(self, upstream_id: str, message: WatermarkMessage) -> None:
        logger.debug(
            "%s <- %s: WATERMARK %s (%d rows)",
            self.config.task_id,
            upstream_id,
            _fmt_wm(message.watermark),
            message.num_rows,
        )
        old_effective = self.effective_watermark
        self._state.upstream_watermarks[upstream_id] = message.watermark
        self._state.upstream_records[upstream_id] = self._state.upstream_records.get(upstream_id, 0) + message.num_rows
        new_effective = self.effective_watermark

        if new_effective is not None and (old_effective is None or new_effective > old_effective):
            if self.config.node_type == "output":
                if self._should_fire():
                    await self._fire()
            else:
                forward_message = WatermarkMessage(
                    watermark=new_effective,
                    num_rows=message.num_rows,
                    batch=message.batch,
                )
                for handle in self.downstream_handles:
                    await handle.send(forward_message)

    async def receive_expiration(self, downstream_id: str, message: ExpirationMessage) -> None:
        logger.debug("%s <- %s: EXPIRATION %s", self.config.task_id, downstream_id, _fmt_wm(message.threshold))
        old_threshold = self.expiration_threshold
        self._downstream_thresholds[downstream_id] = message.threshold
        new_threshold = self.expiration_threshold

        if new_threshold is not None and (old_threshold is None or new_threshold > old_threshold):
            await self._handle_expiration(new_threshold)

    def _should_fire(self) -> bool:
        if not isinstance(self.declaration, OutputDeclaration):
            return False

        if self.declaration.trigger_records is not None:
            if self.total_records - self._state.records_at_last_fire >= self.declaration.trigger_records:
                return True

        if self.declaration.trigger_window is not None:
            watermark = self.effective_watermark
            if watermark is not None and self._state.last_fired_window is not None:
                current_window = self._snap_to_window(watermark)
                if current_window > self._state.last_fired_window:
                    return True

        return False

    async def _fire(self) -> None:
        if not isinstance(self.declaration, OutputDeclaration):
            return

        self._state.records_at_last_fire = self.total_records

        target = None
        if self.declaration.trigger_window is not None and self.effective_watermark is not None:
            target = self._snap_to_window(self.effective_watermark)

        if self.declaration.trigger_window is not None and self._state.last_fired_window is not None:
            batch_start = self._state.last_fired_window - self._max_window_size + self.declaration.trigger_window
            batch_end = (
                target if target is not None else self._state.last_fired_window + self.declaration.trigger_window
            )
            self._state.last_fired_window = batch_end
            self.engine.set_batch_start(batch_start)
            self.engine.set_batch_end(batch_end)
            self.engine.set_window_hop(self.declaration.trigger_window)

        result = self.engine.query(self.declaration.sql)
        await self._emit_by_window(result)
        await self.propagate_expiration()

    async def final_fire(self) -> None:
        if self.config.node_type == "output" and self._should_fire():
            await self._fire()

    async def propagate_expiration(self) -> None:
        if not isinstance(self.declaration, OutputDeclaration):
            return
        if self._state.last_fired_window is None:
            return

        max_window = max(self.declaration.window_sizes, default=dt.timedelta(0))
        threshold = self._state.last_fired_window - max_window
        message = ExpirationMessage(threshold=threshold)

        for handle in self.upstream_handles:
            await handle.send(message)

    async def _emit_by_window(self, result: pa.RecordBatch) -> None:
        if self._sink is None:
            return

        if "window_end" not in result.schema.names or result.num_rows == 0:
            if result.num_rows > 0:
                logger.debug("%s: SINK WRITE %d rows", self.config.task_id, result.num_rows)
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
                logger.debug(
                    "%s: SINK WRITE window_end=%s (%d rows)",
                    self.config.task_id,
                    current_end.as_py().strftime("%H:%M:%S"),
                    batch.num_rows,
                )
                await self._sink.write(batch)
                current_end = end
                start_idx = i

        if start_idx < len(window_ends):
            batch = table.slice(start_idx).to_batches()[0]
            logger.debug(
                "%s: SINK WRITE window_end=%s (%d rows)",
                self.config.task_id,
                current_end.as_py().strftime("%H:%M:%S"),
                batch.num_rows,
            )
            await self._sink.write(batch)

    def _snap_to_window(self, watermark: dt.datetime) -> dt.datetime:
        if not isinstance(self.declaration, OutputDeclaration) or self.declaration.trigger_window is None:
            return watermark

        window_seconds = int(self.declaration.trigger_window.total_seconds())
        midnight = watermark.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_since_midnight = (watermark - midnight).total_seconds()
        snapped_seconds = int(seconds_since_midnight // window_seconds) * window_seconds
        return midnight + dt.timedelta(seconds=snapped_seconds)

    async def _handle_expiration(self, threshold: dt.datetime) -> None:
        if self.config.node_type == "source":
            if isinstance(self.declaration, SourceDeclaration) and self.declaration.ts_col:
                logger.debug("%s: DELETE data before %s", self.config.task_id, _fmt_wm(threshold))
                self.engine.delete_before(self.config.node_name, self.declaration.ts_col, threshold)
        else:
            final_threshold = threshold
            if self.effective_watermark is not None:
                if isinstance(self.declaration, ViewDeclaration) and self.declaration.window_sizes:
                    max_window = max(self.declaration.window_sizes)
                    own_threshold = self.effective_watermark - max_window
                    final_threshold = min(threshold, own_threshold)

            message = ExpirationMessage(threshold=final_threshold)
            for handle in self.upstream_handles:
                await handle.send(message)

    def checkpoint(self) -> TaskState:
        return self._state

    def restore(self, state: TaskState) -> None:
        self._state = state
