import asyncio
import typing
from typing import Any

import pyarrow as pa

from quackflow.connectors.kafka.serializers import JsonSerializer


@typing.runtime_checkable
class Serializer(typing.Protocol):
    def __call__(self, data: dict[str, Any], topic: str, *, is_key: bool = False) -> bytes: ...


class KafkaSink:
    def __init__(
        self,
        topic: str,
        *,
        bootstrap_servers: str,
        value_serializer: Serializer | None = None,
        key_serializer: Serializer | None = None,
        producer_config: dict[str, Any] | None = None,
        _producer: Any = None,
    ):
        from quackflow.connectors.kafka import _check_kafka_deps

        _check_kafka_deps()

        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._value_serializer = value_serializer or JsonSerializer()
        self._key_serializer = key_serializer
        self._producer_config = producer_config or {}
        self._producer = _producer

    async def start(self) -> None:
        if self._producer is None:
            from confluent_kafka import Producer

            config = {
                "bootstrap.servers": self._bootstrap_servers,
                **self._producer_config,
            }
            self._producer = Producer(config)

    async def write(self, batch: pa.RecordBatch) -> None:
        rows = batch.to_pylist()

        messages: list[tuple[bytes, bytes | None]] = []
        for row in rows:
            key_bytes: bytes | None = None
            if self._key_serializer is not None and "__key" in row:
                key_data = row.pop("__key")
                if key_data is not None:
                    key_bytes = self._key_serializer(key_data, self._topic, is_key=True)
            elif "__key" in row:
                row.pop("__key")

            value_bytes = self._value_serializer(row, self._topic)
            messages.append((value_bytes, key_bytes))

        if messages:
            await asyncio.to_thread(self._produce_batch, messages)

    def _produce_batch(self, messages: list[tuple[bytes, bytes | None]]) -> None:
        for value, key in messages:
            while True:
                try:
                    self._producer.produce(self._topic, value=value, key=key)
                    self._producer.poll(0)
                    break
                except BufferError:
                    self._producer.poll(1)
        self._producer.flush()

    async def stop(self) -> None:
        if self._producer is not None:
            await asyncio.to_thread(self._producer.flush)
