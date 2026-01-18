"""Transport abstractions for task-to-task communication."""

import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

if TYPE_CHECKING:
    from quackflow._internal.distributed.flight_client import FlightClientPool
    from quackflow._internal.task import Task


@dataclass
class WatermarkMessage:
    """Message sent downstream with watermark update."""

    watermark: dt.datetime
    num_rows: int
    batch: pa.RecordBatch | None = None  # None in local mode, present in distributed


@dataclass
class ExpirationMessage:
    """Message sent upstream with expiration threshold."""

    threshold: dt.datetime


class DownstreamHandle(Protocol):
    """Handle to send data/watermarks to a downstream task."""

    @property
    def task_id(self) -> str: ...

    @property
    def target_partition_id(self) -> int: ...

    @property
    def target_repartition_key(self) -> list[str] | None: ...

    async def send(self, message: WatermarkMessage) -> None: ...


class UpstreamHandle(Protocol):
    """Handle to send expiration to an upstream task."""

    @property
    def task_id(self) -> str: ...

    async def send(self, message: ExpirationMessage) -> None: ...


class LocalDownstreamHandle:
    """Direct call to local task (single-worker mode)."""

    def __init__(self, sender_id: str, target: "Task"):
        self._sender_id = sender_id
        self._target = target

    @property
    def task_id(self) -> str:
        return self._target.config.task_id

    @property
    def target_partition_id(self) -> int:
        return self._target.config.partition_id

    @property
    def target_repartition_key(self) -> list[str] | None:
        return self._target.config.repartition_key

    async def send(self, message: WatermarkMessage) -> None:
        await self._target.receive_watermark(self._sender_id, message)


class LocalUpstreamHandle:
    """Direct call to local task (single-worker mode)."""

    def __init__(self, sender_id: str, target: "Task"):
        self._sender_id = sender_id
        self._target = target

    @property
    def task_id(self) -> str:
        return self._target.config.task_id

    async def send(self, message: ExpirationMessage) -> None:
        await self._target.receive_expiration(self._sender_id, message)


def _extract_partition_id(task_id: str) -> int:
    """Extract partition ID from task_id like 'results[0]' → 0."""
    return int(task_id.split("[")[1].rstrip("]"))


class RemoteDownstreamHandle:
    """Send messages to a task on a remote worker via Arrow Flight."""

    def __init__(
        self,
        sender_id: str,
        target_task_id: str,
        target_repartition_key: list[str] | None,
        target_host: str,
        target_port: int,
        client_pool: "FlightClientPool",
    ):
        self._sender_id = sender_id
        self._target_task_id = target_task_id
        self._target_repartition_key = target_repartition_key
        self._target_host = target_host
        self._target_port = target_port
        self._client_pool = client_pool

    @property
    def task_id(self) -> str:
        return self._target_task_id

    @property
    def target_partition_id(self) -> int:
        return _extract_partition_id(self._target_task_id)

    @property
    def target_repartition_key(self) -> list[str] | None:
        return self._target_repartition_key

    async def send(self, message: WatermarkMessage) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._client_pool.send_watermark,
            self._target_host,
            self._target_port,
            self._sender_id,
            self._target_task_id,
            message,
        )


class RemoteUpstreamHandle:
    """Send expiration to an upstream task on a remote worker via Arrow Flight."""

    def __init__(
        self,
        sender_id: str,
        target_task_id: str,
        target_host: str,
        target_port: int,
        client_pool: "FlightClientPool",
    ):
        self._sender_id = sender_id
        self._target_task_id = target_task_id
        self._target_host = target_host
        self._target_port = target_port
        self._client_pool = client_pool

    @property
    def task_id(self) -> str:
        return self._target_task_id

    async def send(self, message: ExpirationMessage) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._client_pool.send_expiration,
            self._target_host,
            self._target_port,
            self._sender_id,
            self._target_task_id,
            message,
        )
