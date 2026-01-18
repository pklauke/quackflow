"""Arrow Flight server for receiving messages from remote workers."""

import asyncio
import datetime as dt
import json
import logging
from typing import TYPE_CHECKING

import pyarrow.flight as flight

from quackflow._internal.distributed.flight_client import MSG_TYPE_EXPIRATION, MSG_TYPE_WATERMARK
from quackflow._internal.transport import ExpirationMessage, WatermarkMessage

if TYPE_CHECKING:
    from quackflow._internal.task import Task

logger = logging.getLogger(__name__)


class QuackflowFlightServer(flight.FlightServerBase):
    """Arrow Flight server for receiving messages from remote tasks."""

    def __init__(
        self,
        location: str,
        tasks: dict[str, "Task"],
        loop: asyncio.AbstractEventLoop,
    ):
        super().__init__(location)
        self._tasks = tasks
        self._loop = loop

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.FlightMetadataWriter,
    ) -> None:
        """Receive messages from remote tasks."""
        # Descriptor path contains target task_id
        target_id = descriptor.path[0].decode()

        # Read the chunk with metadata
        chunk = reader.read_chunk()
        metadata_bytes = chunk.app_metadata
        metadata = json.loads(bytes(metadata_bytes).decode())

        msg_type = metadata["msg_type"]
        sender_id = metadata["sender_id"]

        if msg_type == MSG_TYPE_WATERMARK:
            watermark = dt.datetime.fromtimestamp(
                metadata["watermark_us"] / 1_000_000,
                tz=dt.timezone.utc,
            )
            # Get batch if it has columns (not empty metadata-only batch)
            batch = chunk.data if chunk.data.num_columns > 0 else None

            message = WatermarkMessage(
                watermark=watermark,
                num_rows=metadata["num_rows"],
                batch=batch,
            )

            logger.debug(
                "Flight server: %s <- %s WATERMARK %s",
                target_id,
                sender_id,
                watermark.strftime("%H:%M:%S"),
            )

            # Dispatch to task's receive_watermark
            task = self._tasks[target_id]
            future = asyncio.run_coroutine_threadsafe(
                task.receive_watermark(sender_id, message),
                self._loop,
            )
            future.result()  # Wait for completion

        elif msg_type == MSG_TYPE_EXPIRATION:
            threshold = dt.datetime.fromtimestamp(
                metadata["threshold_us"] / 1_000_000,
                tz=dt.timezone.utc,
            )

            message = ExpirationMessage(threshold=threshold)

            logger.debug(
                "Flight server: %s <- %s EXPIRATION %s",
                target_id,
                sender_id,
                threshold.strftime("%H:%M:%S"),
            )

            task = self._tasks[target_id]
            future = asyncio.run_coroutine_threadsafe(
                task.receive_expiration(sender_id, message),
                self._loop,
            )
            future.result()
