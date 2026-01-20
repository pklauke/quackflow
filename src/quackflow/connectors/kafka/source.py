import asyncio
import datetime as dt
import json
import logging
from collections.abc import Callable
from typing import Any

import pyarrow as pa

from quackflow.schema import Schema
from quackflow.time_notion import TimeNotion

logger = logging.getLogger(__name__)


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
        deserializer: Callable[[bytes], dict[str, Any]] | None = None,  # TODO
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
        self._deserializer = deserializer or (lambda b: json.loads(b.decode("utf-8")))
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

            self._consumer = Consumer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "group.id": self._group_id,
                    "auto.offset.reset": self._auto_offset_reset,
                }
            )
        await asyncio.to_thread(self._consumer.subscribe, [self._topic])

    async def read(self) -> pa.RecordBatch:
        messages: list[dict[str, Any]] = []

        for _ in range(self._batch_size):
            msg = await asyncio.to_thread(self._consumer.poll, self._poll_timeout)
            if msg is None:
                break
            if msg.error():
                continue
            try:
                data = self._deserializer(msg.value())
                messages.append(data)
            except Exception:
                logger.warning("Failed to deserialize message, skipping")
                continue

        if not messages:
            field_names = list(self._schema.fields().keys())
            return pa.RecordBatch.from_pydict({name: [] for name in field_names})

        batch = pa.RecordBatch.from_pylist(messages)  # TODO inefficient? only pylist?
        self._update_watermark(batch)
        return batch

    async def stop(self) -> None:
        if self._consumer is not None:
            await asyncio.to_thread(self._consumer.close)

    async def seek(self, timestamp: dt.datetime) -> None:
        from confluent_kafka import TopicPartition

        assignment = await asyncio.to_thread(self._consumer.assignment)
        ts_ms = int(timestamp.timestamp() * 1000)
        partitions = [TopicPartition(tp.topic, tp.partition, ts_ms) for tp in assignment]
        offsets = await asyncio.to_thread(self._consumer.offsets_for_times, partitions)
        for tp in offsets:
            if tp.offset >= 0:
                await asyncio.to_thread(self._consumer.seek, tp)
