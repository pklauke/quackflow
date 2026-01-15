"""Transport abstractions for task-to-task communication."""

import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

if TYPE_CHECKING:
    from quackflow.distributed.flight_client import FlightClientPool
    from quackflow.task import Task


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


class RemoteDownstreamHandle:
    """Send messages to a task on a remote worker via Arrow Flight."""

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
