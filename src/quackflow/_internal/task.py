"""Task - logical execution unit that runs one node's SQL for specific partition(s)."""

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from quackflow.app import OutputDeclaration, SourceDeclaration, ViewDeclaration
from quackflow._internal.execution import TaskConfig
from quackflow._internal.repartition import repartition
from quackflow._internal.transport import (
    DownstreamHandle,
    ExpirationMessage,
    UpstreamHandle,
    WatermarkMessage,
)

if TYPE_CHECKING:
    from quackflow._internal.engine import Engine
    from quackflow.sink import Sink
    from quackflow.source import ReplayableSource, Source

logger = logging.getLogger(__name__)


@dataclass
class TaskState:
    upstream_watermarks: dict[str, dt.datetime] = field(default_factory=dict)
    upstream_records: dict[str, int] = field(default_factory=dict)
    last_fired_window: dt.datetime | None = None
    records_at_last_fire: int = 0


def _fmt_wm(watermark: dt.datetime) -> str:
    return watermark.strftime("%H:%M:%S")


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

        # Distributed mode settings
        self._num_partitions: int = 1
        self._propagate_batch: bool = False  # Whether to include batch in messages

        # Trigger settings (from declaration or inferred)
        self._trigger_window: dt.timedelta | None = None
        self._trigger_records: int | None = None

    def set_source(self, source: "Source") -> None:
        self._source = source

    def set_sink(self, sink: "Sink") -> None:
        self._sink = sink

    def set_max_window_size(self, size: dt.timedelta) -> None:
        self._max_window_size = size

    def set_num_partitions(self, num_partitions: int) -> None:
        self._num_partitions = num_partitions

    def set_propagate_batch(self, enabled: bool) -> None:
        self._propagate_batch = enabled

    def set_trigger(self, window: dt.timedelta | None, records: int | None) -> None:
        self._trigger_window = window
        self._trigger_records = records

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
        if self._trigger_window is not None:
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
                        # Check if any handle needs repartitioning
                        own_key = (
                            self.declaration.partition_by if isinstance(self.declaration, SourceDeclaration) else None
                        )
                        repartition_key = None
                        for handle in self.downstream_handles:
                            target_key = handle.target_repartition_key
                            if target_key and (own_key is None or set(target_key) != set(own_key)):
                                repartition_key = target_key
                                break

                        if repartition_key:
                            partitioned = repartition(batch, repartition_key, self._num_partitions)
                            for handle in self.downstream_handles:
                                partition_batch = partitioned.get(handle.target_partition_id)
                                if partition_batch is not None:
                                    message = WatermarkMessage(
                                        watermark=watermark,
                                        num_rows=partition_batch.num_rows,
                                        batch=partition_batch if self._propagate_batch else None,
                                    )
                                    await handle.send(message)
                        else:
                            message = WatermarkMessage(
                                watermark=watermark,
                                num_rows=batch.num_rows,
                                batch=batch if self._propagate_batch else None,
                            )
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

        # In distributed mode, insert received batch into our engine
        if message.batch is not None:
            table_name = upstream_id.split("[")[0]
            self.engine.insert(table_name, message.batch)

        old_effective = self.effective_watermark
        self._state.upstream_watermarks[upstream_id] = message.watermark
        self._state.upstream_records[upstream_id] = self._state.upstream_records.get(upstream_id, 0) + message.num_rows
        new_effective = self.effective_watermark

        if new_effective is not None and (old_effective is None or new_effective > old_effective):
            if isinstance(self.declaration, ViewDeclaration) and not self._propagate_batch:
                # Single-worker mode: views forward watermarks directly (engine has actual DuckDB view)
                await self._forward_watermark(message)
            elif self._should_fire():
                # Distributed mode or output: check trigger and fire
                await self._fire()

    async def receive_expiration(self, downstream_id: str, message: ExpirationMessage) -> None:
        logger.debug("%s <- %s: EXPIRATION %s", self.config.task_id, downstream_id, _fmt_wm(message.threshold))
        old_threshold = self.expiration_threshold
        self._downstream_thresholds[downstream_id] = message.threshold
        new_threshold = self.expiration_threshold

        if new_threshold is not None and (old_threshold is None or new_threshold > old_threshold):
            await self._handle_expiration(new_threshold)

    def _should_fire(self) -> bool:
        # Check records trigger
        if self._trigger_records is not None:
            if self.total_records - self._state.records_at_last_fire >= self._trigger_records:
                return True

        # Check window trigger
        if self._trigger_window is not None:
            watermark = self.effective_watermark
            if watermark is not None and self._state.last_fired_window is not None:
                current_window = self._snap_to_window(watermark)
                if current_window > self._state.last_fired_window:
                    return True

        return False

    async def _fire(self) -> None:
        self._state.records_at_last_fire = self.total_records

        # Set batch window parameters if window trigger is configured
        target = None
        if self._trigger_window is not None and self.effective_watermark is not None:
            target = self._snap_to_window(self.effective_watermark)

        if self._trigger_window is not None and self._state.last_fired_window is not None:
            batch_start = self._state.last_fired_window - self._max_window_size + self._trigger_window
            batch_end = target if target is not None else self._state.last_fired_window + self._trigger_window
            self._state.last_fired_window = batch_end
            self.engine.set_batch_start(batch_start)
            self.engine.set_batch_end(batch_end)
            self.engine.set_window_hop(self._trigger_window)

        if isinstance(self.declaration, OutputDeclaration):
            # Output: query, write to sink, propagate expiration
            result = self.engine.query(self.declaration.sql)
            await self._emit_by_window(result)
            await self.propagate_expiration()
        elif isinstance(self.declaration, ViewDeclaration):
            # View: query, send transformed result downstream
            result = self.engine.query(self.declaration.sql)
            await self._send_downstream(result)

    async def _send_downstream(self, batch: pa.RecordBatch) -> None:
        """Send a batch to all downstream handles (used by views after firing)."""
        if batch.num_rows == 0:
            return

        watermark = self.effective_watermark
        if watermark is None:
            return

        # Check if repartitioning is needed
        own_key = self.declaration.partition_by if isinstance(self.declaration, ViewDeclaration) else None
        repartition_key = None
        for handle in self.downstream_handles:
            target_key = handle.target_repartition_key
            if target_key and (own_key is None or set(target_key) != set(own_key)):
                repartition_key = target_key
                break

        if repartition_key:
            partitioned = repartition(batch, repartition_key, self._num_partitions)
            for handle in self.downstream_handles:
                partition_batch = partitioned.get(handle.target_partition_id)
                if partition_batch is not None:
                    message = WatermarkMessage(
                        watermark=watermark,
                        num_rows=partition_batch.num_rows,
                        batch=partition_batch if self._propagate_batch else None,
                    )
                    await handle.send(message)
        else:
            message = WatermarkMessage(
                watermark=watermark,
                num_rows=batch.num_rows,
                batch=batch if self._propagate_batch else None,
            )
            for handle in self.downstream_handles:
                await handle.send(message)

    async def _forward_watermark(self, message: WatermarkMessage) -> None:
        """Forward watermark to downstream handles (single-worker mode for views)."""
        for handle in self.downstream_handles:
            await handle.send(message)

    async def final_fire(self) -> None:
        if self._should_fire():
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
        if self._trigger_window is None:
            return watermark

        window_seconds = int(self._trigger_window.total_seconds())
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
