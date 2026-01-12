"""Task - logical execution unit that runs one node's SQL for specific partition(s)."""

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from quackflow.app import DataPacket, OutputDeclaration, SourceDeclaration, ViewDeclaration
from quackflow.execution import TaskConfig

logger = logging.getLogger(__name__)


def _fmt_wm(watermark: dt.datetime) -> str:
    """Format watermark for log output."""
    return watermark.strftime("%H:%M:%S")


if TYPE_CHECKING:
    from quackflow.engine import Engine
    from quackflow.sink import Sink
    from quackflow.source import ReplayableSource, Source


@dataclass
class TaskState:
    """All mutable state for a task, designed for future checkpointing."""

    # Watermarks received from each upstream task
    upstream_watermarks: dict[str, dt.datetime] = field(default_factory=dict)
    # Records received from each upstream task
    upstream_records: dict[str, int] = field(default_factory=dict)
    # For window-based triggers
    last_fired_window: dt.datetime | None = None
    # Records at last fire (for records-based triggers)
    records_at_last_fire: int = 0


class Task:
    """Executes one node's logic for a specific partition."""

    def __init__(
        self,
        config: TaskConfig,
        declaration: SourceDeclaration | ViewDeclaration | OutputDeclaration,
        engine: "Engine",
        input_queues: dict[str, "asyncio.Queue[DataPacket | None]"],
        output_queues: dict[str, "asyncio.Queue[DataPacket | None]"],
        expiration_input_queues: dict[str, "asyncio.Queue[dt.datetime | None]"] | None = None,
        expiration_output_queues: dict[str, "asyncio.Queue[dt.datetime | None]"] | None = None,
    ):
        self.config = config
        self.declaration = declaration
        self.engine = engine
        self.input_queues = input_queues
        self.output_queues = output_queues
        self.expiration_input_queues = expiration_input_queues or {}
        self.expiration_output_queues = expiration_output_queues or {}

        # All mutable state in one object for future checkpointing
        self._state = TaskState()

        # External components (set during setup)
        self._source: "Source | None" = None
        self._sink: "Sink | None" = None
        self._max_window_size: dt.timedelta = dt.timedelta(0)

        # Track expiration thresholds from downstream
        self._downstream_thresholds: dict[str, dt.datetime] = {}

    def set_source(self, source: "Source") -> None:
        self._source = source

    def set_sink(self, sink: "Sink") -> None:
        self._sink = sink

    def set_max_window_size(self, size: dt.timedelta) -> None:
        self._max_window_size = size

    @property
    def effective_watermark(self) -> dt.datetime | None:
        """Effective watermark (min of all upstream watermarks)."""
        if not self._state.upstream_watermarks:
            return None
        if len(self._state.upstream_watermarks) < len(self.config.upstream_tasks):
            return None
        return min(self._state.upstream_watermarks.values())

    @property
    def total_records(self) -> int:
        return sum(self._state.upstream_records.values())

    @property
    def expiration_threshold(self) -> dt.datetime | None:
        """Minimum expiration threshold from all downstream tasks."""
        if not self._downstream_thresholds:
            return None
        if len(self._downstream_thresholds) < len(self.config.downstream_tasks):
            return None
        return min(self._downstream_thresholds.values())

    def initialize(self, start: dt.datetime) -> None:
        """Initialize task state."""
        if isinstance(self.declaration, OutputDeclaration):
            if self.declaration.trigger_window is not None:
                self._state.last_fired_window = self._snap_to_window(start)

    async def run(self, start: dt.datetime, end: dt.datetime | None) -> None:
        """Main task loop."""
        if self.config.node_type == "source":
            await self._run_source(start, end)
        else:
            await self._run_processor()

    async def _run_source(self, start: dt.datetime, end: dt.datetime | None) -> None:
        """Run source ingestion loop."""
        from quackflow.source import ReplayableSource

        source = self._source
        assert source is not None

        if isinstance(source, ReplayableSource):
            await source.seek(start)
        await source.start()

        try:
            while True:
                # Check for expiration messages
                await self._check_expiration_queues()

                if end is not None:
                    watermark = source.watermark
                    if watermark is not None and watermark >= end:
                        break

                batch = await source.read()
                if batch.num_rows > 0:
                    self.engine.insert(self.config.node_name, batch)
                    watermark = source.watermark
                    if watermark is not None:
                        packet = DataPacket(batch=batch, watermark=watermark)
                        await self._send_downstream(packet)

                if batch.num_rows == 0:
                    # Small delay to avoid busy loop
                    await asyncio.sleep(0.01)
                    # Check if we should stop
                    watermark = source.watermark
                    if end is not None and watermark is not None and watermark >= end:
                        break
        finally:
            await source.stop()
            # Signal completion to downstream
            await self._send_completion()

            # Wait for expiration completion from all downstream tasks
            await self._wait_for_expiration_completion()

    async def _run_processor(self) -> None:
        """Run view/output processing loop."""
        pending_upstreams = set(self.config.upstream_tasks)

        while pending_upstreams:
            # Check for expiration messages
            await self._check_expiration_queues()

            # Read from all input queues
            for upstream_id in list(pending_upstreams):
                if upstream_id not in self.input_queues:
                    pending_upstreams.discard(upstream_id)
                    continue

                queue = self.input_queues[upstream_id]
                try:
                    packet = queue.get_nowait()
                    if packet is None:
                        pending_upstreams.discard(upstream_id)
                    else:
                        await self._receive(upstream_id, packet)
                except asyncio.QueueEmpty:
                    pass

            if pending_upstreams:
                await asyncio.sleep(0.001)

        # Final fire for outputs
        if self.config.node_type == "output":
            if self._should_fire():
                await self._fire()

        # Signal completion to downstream (data)
        await self._send_completion()

        # Signal completion to upstream (expiration)
        await self._send_expiration_completion()

    async def _receive(self, upstream_id: str, packet: DataPacket) -> None:
        """Process a received data packet."""
        logger.debug(
            "%s <- %s: WATERMARK %s (%d rows)",
            self.config.task_id,
            upstream_id,
            _fmt_wm(packet.watermark),
            packet.batch.num_rows,
        )
        old_effective = self.effective_watermark
        self._state.upstream_watermarks[upstream_id] = packet.watermark
        self._state.upstream_records[upstream_id] = (
            self._state.upstream_records.get(upstream_id, 0) + packet.batch.num_rows
        )
        new_effective = self.effective_watermark

        # Check if we should fire (for outputs) or propagate (for views)
        if new_effective is not None and (old_effective is None or new_effective > old_effective):
            if self.config.node_type == "output":
                if self._should_fire():
                    await self._fire()
            else:
                # View: propagate downstream
                await self._send_downstream(DataPacket(batch=packet.batch, watermark=new_effective))

    def _should_fire(self) -> bool:
        """Check if output should fire."""
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
        """Fire the output query and emit results."""
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
        await self._propagate_expiration()

    async def _emit_by_window(self, result: pa.RecordBatch) -> None:
        """Emit results grouped by window_end in order."""
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
        """Snap watermark down to the previous window boundary."""
        if not isinstance(self.declaration, OutputDeclaration) or self.declaration.trigger_window is None:
            return watermark

        window_seconds = int(self.declaration.trigger_window.total_seconds())
        midnight = watermark.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_since_midnight = (watermark - midnight).total_seconds()
        snapped_seconds = int(seconds_since_midnight // window_seconds) * window_seconds
        return midnight + dt.timedelta(seconds=snapped_seconds)

    async def _send_downstream(self, packet: DataPacket) -> None:
        """Send packet to all downstream tasks."""
        for queue in self.output_queues.values():
            await queue.put(packet)

    async def _send_completion(self) -> None:
        """Signal completion to downstream tasks."""
        for queue in self.output_queues.values():
            await queue.put(None)

    async def _send_expiration_completion(self) -> None:
        """Signal expiration completion to upstream tasks."""
        for queue in self.expiration_output_queues.values():
            await queue.put(None)

    async def _wait_for_expiration_completion(self) -> None:
        """Wait for expiration completion signals from all downstream tasks."""
        pending_downstream = set(self.expiration_input_queues.keys())

        while pending_downstream:
            for downstream_id in list(pending_downstream):
                queue = self.expiration_input_queues[downstream_id]
                try:
                    msg = queue.get_nowait()
                    if msg is None:
                        # Completion signal
                        pending_downstream.discard(downstream_id)
                    else:
                        # Expiration threshold - process it
                        await self.receive_expiration(downstream_id, msg)
                except asyncio.QueueEmpty:
                    pass

            if pending_downstream:
                await asyncio.sleep(0.001)

    async def _check_expiration_queues(self) -> None:
        """Check and process any pending expiration messages."""
        for downstream_id, queue in self.expiration_input_queues.items():
            try:
                threshold = queue.get_nowait()
                if threshold is not None:
                    await self.receive_expiration(downstream_id, threshold)
            except asyncio.QueueEmpty:
                pass

    async def _propagate_expiration(self) -> None:
        """Propagate expiration threshold to upstream tasks."""
        if not isinstance(self.declaration, OutputDeclaration):
            return
        if self._state.last_fired_window is None:
            return

        max_window = max(self.declaration.window_sizes, default=dt.timedelta(0))
        threshold = self._state.last_fired_window - max_window

        for queue in self.expiration_output_queues.values():
            await queue.put(threshold)

    async def receive_expiration(self, downstream_id: str, threshold: dt.datetime) -> None:
        """Receive expiration threshold from a downstream task."""
        logger.debug(
            "%s <- %s: EXPIRATION %s",
            self.config.task_id,
            downstream_id,
            _fmt_wm(threshold),
        )
        old_threshold = self.expiration_threshold
        self._downstream_thresholds[downstream_id] = threshold
        new_threshold = self.expiration_threshold

        if new_threshold is not None and (old_threshold is None or new_threshold > old_threshold):
            await self._handle_expiration(new_threshold)

    async def _handle_expiration(self, threshold: dt.datetime) -> None:
        """Handle expiration - delete data for sources, propagate for views."""
        if self.config.node_type == "source":
            # Source: delete expired data
            if isinstance(self.declaration, SourceDeclaration) and self.declaration.ts_col:
                logger.debug("%s: DELETE data before %s", self.config.task_id, _fmt_wm(threshold))
                self.engine.delete_before(self.config.node_name, self.declaration.ts_col, threshold)
        else:
            # View: calculate own threshold based on watermark and window size
            # Then propagate the more conservative (earlier) threshold upstream
            final_threshold = threshold
            if self.effective_watermark is not None:
                if isinstance(self.declaration, ViewDeclaration) and self.declaration.window_sizes:
                    max_window = max(self.declaration.window_sizes)
                    own_threshold = self.effective_watermark - max_window
                    final_threshold = min(threshold, own_threshold)
                    if final_threshold != threshold:
                        logger.debug(
                            "%s: ADJUST expiration %s -> %s (own=%s, wm=%s)",
                            self.config.task_id,
                            _fmt_wm(threshold),
                            _fmt_wm(final_threshold),
                            _fmt_wm(own_threshold),
                            _fmt_wm(self.effective_watermark),
                        )

            for queue in self.expiration_output_queues.values():
                await queue.put(final_threshold)

    def get_expiration_threshold(self) -> dt.datetime | None:
        """Get the expiration threshold for upstream data cleanup."""
        if not isinstance(self.declaration, OutputDeclaration):
            return None
        if self._state.last_fired_window is None:
            return None

        max_window = max(self.declaration.window_sizes, default=dt.timedelta(0))
        return self._state.last_fired_window - max_window

    # Future: checkpointing support
    def checkpoint(self) -> TaskState:
        """Return current state for checkpointing."""
        return self._state

    def restore(self, state: TaskState) -> None:
        """Restore state from checkpoint."""
        self._state = state
