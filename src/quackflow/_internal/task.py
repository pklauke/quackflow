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
    last_fired_watermark: dt.datetime | None = None
    records_at_last_fire: int = 0


def _fmt_wm(watermark: dt.datetime) -> str:
    if watermark.tzinfo is not None:
        watermark = watermark.astimezone(dt.timezone.utc)
    return watermark.isoformat()


class Task:
    def __init__(
        self,
        config: TaskConfig,
        declaration: SourceDeclaration | ViewDeclaration | OutputDeclaration,
        engine: "Engine",
        *,
        source: "Source | None" = None,
        sink: "Sink | None" = None,
        max_window_size: dt.timedelta = dt.timedelta(0),
        num_partitions: int = 1,
        propagate_batch: bool = False,
    ):
        self.config = config
        self.declaration = declaration
        self._ctx = engine.create_context()
        self._state = TaskState()

        self.downstream_handles: list[DownstreamHandle] = []
        self.upstream_handles: list[UpstreamHandle] = []

        self._source = source
        self._sink = sink
        self._max_window_size = max_window_size
        self._num_partitions = num_partitions
        self._propagate_batch = propagate_batch
        self._downstream_expirations: dict[str, ExpirationMessage] = {}
        self._fire_lock = asyncio.Lock()
        self._end: dt.datetime | None = None

        if declaration._trigger is not None:
            self._trigger_window = declaration._trigger.window
            self._trigger_records = declaration._trigger.records
        else:
            self._trigger_window = None
            self._trigger_records = None

        if isinstance(declaration, (ViewDeclaration, OutputDeclaration)):
            self._sql = declaration.sql
            self._window_sizes = declaration.window_sizes
        else:
            self._sql = None
            self._window_sizes = []

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

    def initialize(self, start: dt.datetime, end: dt.datetime | None = None) -> None:
        self._end = end
        if self._trigger_window is not None:
            # Initialize to one trigger window before start so first fire can produce window_end=start
            self._state.last_fired_watermark = self._snap_to_window(start) - self._trigger_window

    async def run_source(self, start: dt.datetime, end: dt.datetime | None) -> None:
        from quackflow.source import ReplayableSource

        source = self._source
        assert source is not None

        await source.start()
        if isinstance(source, ReplayableSource):
            # Seek to start - max_window_size to have data for the first window
            seek_timestamp = start - self._max_window_size
            await source.seek(seek_timestamp)

        try:
            while True:
                if end is not None:
                    watermark = source.watermark
                    if watermark is not None and watermark >= end:
                        break

                batch = await source.read()
                if batch.num_rows > 0:
                    await asyncio.to_thread(self._ctx.insert, self.config.node_name, batch)
                    watermark = source.watermark
                    if watermark is not None:
                        await self._send_batch_to_handles(batch, watermark)

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
            self._ctx.insert_or_create(table_name, message.batch)

        old_effective = self.effective_watermark
        self._state.upstream_watermarks[upstream_id] = message.watermark
        self._state.upstream_records[upstream_id] = self._state.upstream_records.get(upstream_id, 0) + message.num_rows
        new_effective = self.effective_watermark

        if new_effective is not None and (old_effective is None or new_effective > old_effective):
            if self._propagate_batch or self._sink is not None:
                # Distributed mode or output: check trigger and fire
                if self._should_fire():
                    await self._fire()
            else:
                # Single-worker views: forward effective watermark (min of all upstreams)
                effective_message = WatermarkMessage(
                    watermark=new_effective,
                    num_rows=message.num_rows,
                    batch=message.batch,
                )
                await self._forward_watermark(effective_message)

    async def receive_expiration(self, downstream_id: str, message: ExpirationMessage) -> None:
        logger.debug(
            "%s <- %s: EXPIRATION processed_until=%s threshold=%s",
            self.config.task_id,
            downstream_id,
            _fmt_wm(message.processed_until),
            _fmt_wm(message.threshold),
        )

        old_message = self._downstream_expirations.get(downstream_id)
        self._downstream_expirations[downstream_id] = message

        # Only process when we have messages from ALL downstreams
        if len(self._downstream_expirations) < len(self.downstream_handles):
            return

        # Compute minimum threshold across all downstreams
        min_threshold = min(m.threshold for m in self._downstream_expirations.values())
        # Use max processed_until as reference point for upstream propagation
        max_processed_until = max(m.processed_until for m in self._downstream_expirations.values())

        # Only process if threshold has advanced
        if old_message is None or message.threshold > old_message.threshold:
            combined_message = ExpirationMessage(
                processed_until=max_processed_until,
                threshold=min_threshold,
            )
            await self._handle_expiration(combined_message)

    def _should_fire(self) -> bool:
        if self._trigger_records is not None:
            if self.total_records - self._state.records_at_last_fire >= self._trigger_records:
                return True

        if self._trigger_window is not None:
            watermark = self.effective_watermark
            if watermark is not None and self._state.last_fired_watermark is not None:
                current_window = self._snap_to_window(watermark)
                if current_window > self._state.last_fired_watermark:
                    return True

        return False

    async def _fire(self) -> None:
        assert self._sql is not None

        async with self._fire_lock:
            if not self._should_fire():
                return

            total_records = self.total_records
            effective_wm = self.effective_watermark
            last_fired = self._state.last_fired_watermark

            self._state.records_at_last_fire = total_records
            target: dt.datetime | None = None

            if self._trigger_window is not None and last_fired is not None and effective_wm is not None:
                target = self._snap_to_window(effective_wm)
                if self._end is not None:
                    end_snapped = self._snap_to_window(self._end)
                    target = min(target, end_snapped)
                if target > last_fired:
                    batch_start = last_fired - self._max_window_size + self._trigger_window
                    logger.debug(
                        "%s: FIRE batch_start=%s batch_end=%s",
                        self.config.task_id,
                        _fmt_wm(batch_start),
                        _fmt_wm(target),
                    )
                    result = await asyncio.to_thread(
                        self._ctx.query_with_window,
                        self._sql,
                        batch_start,
                        target,
                        self._trigger_window,
                    )
                else:
                    result = await asyncio.to_thread(self._ctx.query, self._sql)
            else:
                result = await asyncio.to_thread(self._ctx.query, self._sql)

            logger.debug("%s: query returned %d rows", self.config.task_id, result.num_rows)

            if self._sink is not None:
                await self._emit_by_window(result)
                if target is not None:
                    self._state.last_fired_watermark = target
                await self.propagate_expiration()
            else:
                await self._send_downstream(result)
                if target is not None:
                    self._state.last_fired_watermark = target

    async def _send_downstream(self, batch: pa.RecordBatch) -> None:
        """Send a batch to all downstream handles (used by views after firing)."""
        if batch.num_rows == 0:
            return

        watermark = self.effective_watermark
        if watermark is None:
            return

        await self._send_batch_to_handles(batch, watermark)

    async def _send_batch_to_handles(self, batch: pa.RecordBatch, watermark: dt.datetime) -> None:
        """Send batch to downstream handles, repartitioning if needed."""
        own_key = getattr(self.declaration, "partition_by", None)
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
        if self._sink is None:
            return
        if self._state.last_fired_watermark is None:
            return

        max_window = max(self._window_sizes, default=dt.timedelta(0))
        threshold = self._state.last_fired_watermark - max_window

        message = ExpirationMessage(
            processed_until=self._state.last_fired_watermark,
            threshold=threshold,
        )

        for handle in self.upstream_handles:
            await handle.send(message)

    async def _emit_by_window(self, result: pa.RecordBatch) -> None:
        if self._sink is None:
            return

        if result.num_rows == 0:
            return

        if "window_end" not in result.schema.names:
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
                    _fmt_wm(current_end.as_py()),
                    batch.num_rows,
                )
                await self._sink.write(batch)
                current_end = end
                start_idx = i

        if start_idx < len(window_ends):
            assert current_end is not None
            batch = table.slice(start_idx).to_batches()[0]
            logger.debug(
                "%s: SINK WRITE window_end=%s (%d rows)",
                self.config.task_id,
                _fmt_wm(current_end.as_py()),
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

    async def _handle_expiration(self, message: ExpirationMessage) -> None:
        if self.config.node_type == "source":
            ts_col = getattr(self.declaration, "ts_col", None)
            if ts_col:
                logger.debug("%s: DELETE data before %s", self.config.task_id, _fmt_wm(message.threshold))
                self._ctx.delete_before(self.config.node_name, ts_col, message.threshold)
        else:
            # Compute this node's threshold from the reference point (avoids compounding)
            window_sizes = getattr(self.declaration, "window_sizes", [])
            max_window = max(window_sizes, default=dt.timedelta(0))
            own_threshold = message.processed_until - max_window

            # Forward minimum of received and own threshold
            forward_message = ExpirationMessage(
                processed_until=message.processed_until,
                threshold=min(message.threshold, own_threshold),
            )
            for handle in self.upstream_handles:
                await handle.send(forward_message)
