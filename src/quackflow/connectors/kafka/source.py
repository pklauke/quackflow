import asyncio
import datetime as dt
import logging
import typing
from typing import Any

import pyarrow as pa

from quackflow.connectors.kafka.deserializers import JsonDeserializer
from quackflow.schema import Schema
from quackflow.time_notion import TimeNotion

logger = logging.getLogger(__name__)


@typing.runtime_checkable
class Deserializer(typing.Protocol):
    def __call__(self, data: bytes, topic: str, *, is_key: bool = False) -> Any: ...


class KafkaSource:
    def __init__(
        self,
        topic: str,
        time_notion: TimeNotion,
        bootstrap_servers: str,
        group_id: str,
        schema: type[Schema],
        *,
        auto_offset_reset: str = "earliest",
        poll_timeout: float = 1.0,
        batch_size: int = 1000,
        value_deserializer: Deserializer | None = None,
        key_deserializer: Deserializer | None = None,
        consumer_config: dict[str, Any] | None = None,
        _consumer: Any = None,
    ):
        from quackflow.connectors.kafka import _check_kafka_deps

        _check_kafka_deps()

        self._topic = topic
        self._time_notion = time_notion
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._schema = schema
        self._auto_offset_reset = auto_offset_reset
        self._poll_timeout = poll_timeout
        self._batch_size = batch_size
        self._value_deserializer = value_deserializer or JsonDeserializer()
        self._key_deserializer = key_deserializer
        self._consumer_config = consumer_config or {}
        self._watermark: dt.datetime | None = None
        self._consumer = _consumer

    @property
    def watermark(self) -> dt.datetime | None:
        return self._watermark

    def _update_watermark(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows == 0:
            return
        new_watermark = self._time_notion.compute_watermark(batch)
        if self._watermark is None or new_watermark > self._watermark:
            self._watermark = new_watermark

    async def start(self) -> None:
        if self._consumer is None:
            from confluent_kafka import Consumer

            config = {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": self._group_id,
                "auto.offset.reset": self._auto_offset_reset,
                **self._consumer_config,
            }
            self._consumer = Consumer(config)

        await asyncio.to_thread(self._consumer.subscribe, [self._topic])

    async def read(self) -> pa.RecordBatch:
        messages: list[dict[str, Any]] = []

        for _ in range(self._batch_size):
            msg = await asyncio.to_thread(self._consumer.poll, self._poll_timeout)
            if msg is None:
                break
            if msg.error():
                logger.debug("Kafka message error: %s", msg.error())
                continue
            try:
                data = self._value_deserializer(msg.value(), self._topic)
                if self._key_deserializer is not None:
                    key_bytes = msg.key()
                    if key_bytes is not None:
                        data["__key"] = self._key_deserializer(key_bytes, self._topic, is_key=True)
                    else:
                        data["__key"] = None
                messages.append(data)
            except Exception:
                logger.warning("Failed to deserialize message, skipping", exc_info=True)
                continue

        if not messages:
            field_names = list(self._schema.fields().keys())
            return pa.RecordBatch.from_pydict({name: [] for name in field_names})

        batch = pa.RecordBatch.from_pylist(messages)
        self._update_watermark(batch)
        return batch

    async def stop(self) -> None:
        if self._consumer is not None:
            await asyncio.to_thread(self._consumer.close)

    async def seek(self, timestamp: dt.datetime) -> None:
        from confluent_kafka import TopicPartition

        # Wait for partition assignment (poll triggers rebalance)
        assignment: list = []
        for _ in range(30):
            assignment = await asyncio.to_thread(self._consumer.assignment)
            if assignment:
                break
            await asyncio.to_thread(self._consumer.poll, 1.0)

        if not assignment:
            logger.warning("No partition assignment received for %s, skipping seek", self._topic)
            return

        ts_ms = int(timestamp.timestamp() * 1000)
        partitions = [TopicPartition(tp.topic, tp.partition, ts_ms) for tp in assignment]
        offsets = await asyncio.to_thread(self._consumer.offsets_for_times, partitions)
        for tp in offsets:
            if tp.offset >= 0:
                await asyncio.to_thread(self._consumer.seek, tp)
