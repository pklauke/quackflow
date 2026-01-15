"""Arrow Flight client for sending messages to remote workers."""

import datetime as dt
import json

import pyarrow as pa
import pyarrow.flight as flight

from quackflow.transport import ExpirationMessage, WatermarkMessage

# Message type discriminators
MSG_TYPE_WATERMARK = 1
MSG_TYPE_EXPIRATION = 2


def _serialize_watermark_metadata(
    sender_id: str,
    target_id: str,
    message: WatermarkMessage,
) -> bytes:
    """Serialize watermark message metadata to JSON bytes."""
    metadata = {
        "msg_type": MSG_TYPE_WATERMARK,
        "sender_id": sender_id,
        "target_id": target_id,
        "watermark_us": int(message.watermark.timestamp() * 1_000_000),
        "num_rows": message.num_rows,
    }
    return json.dumps(metadata).encode()


def _serialize_expiration_metadata(
    sender_id: str,
    target_id: str,
    message: ExpirationMessage,
) -> bytes:
    """Serialize expiration message metadata to JSON bytes."""
    metadata = {
        "msg_type": MSG_TYPE_EXPIRATION,
        "sender_id": sender_id,
        "target_id": target_id,
        "threshold_us": int(message.threshold.timestamp() * 1_000_000),
    }
    return json.dumps(metadata).encode()


class FlightClientPool:
    """Pool of Arrow Flight clients for sending messages to remote workers."""

    def __init__(self):
        self._clients: dict[str, flight.FlightClient] = {}

    def _get_client(self, host: str, port: int) -> flight.FlightClient:
        """Get or create a Flight client for the given endpoint."""
        key = f"{host}:{port}"
        if key not in self._clients:
            self._clients[key] = flight.connect(f"grpc://{host}:{port}")
        return self._clients[key]

    def send_watermark(
        self,
        host: str,
        port: int,
        sender_id: str,
        target_id: str,
        message: WatermarkMessage,
    ) -> None:
        """Send watermark message to remote task via Arrow Flight."""
        client = self._get_client(host, port)
        metadata = _serialize_watermark_metadata(sender_id, target_id, message)

        # Use FlightDescriptor with target task ID as path
        descriptor = flight.FlightDescriptor.for_path(target_id.encode())

        if message.batch is not None:
            # Send batch with metadata
            writer, _ = client.do_put(descriptor, message.batch.schema)
            writer.write_with_metadata(message.batch, metadata)
            writer.close()
        else:
            # Metadata-only message - use empty batch
            empty_schema = pa.schema([])
            empty_batch = pa.RecordBatch.from_pydict({}, schema=empty_schema)
            writer, _ = client.do_put(descriptor, empty_schema)
            writer.write_with_metadata(empty_batch, metadata)
            writer.close()

    def send_expiration(
        self,
        host: str,
        port: int,
        sender_id: str,
        target_id: str,
        message: ExpirationMessage,
    ) -> None:
        """Send expiration message to remote task via Arrow Flight."""
        client = self._get_client(host, port)
        metadata = _serialize_expiration_metadata(sender_id, target_id, message)

        descriptor = flight.FlightDescriptor.for_path(target_id.encode())
        empty_schema = pa.schema([])
        empty_batch = pa.RecordBatch.from_pydict({}, schema=empty_schema)

        writer, _ = client.do_put(descriptor, empty_schema)
        writer.write_with_metadata(empty_batch, metadata)
        writer.close()

    def close(self) -> None:
        """Close all Flight client connections."""
        for client in self._clients.values():
            client.close()
        self._clients.clear()
