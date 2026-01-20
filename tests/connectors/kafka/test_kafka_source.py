import datetime as dt
import json
from typing import Any

import pyarrow as pa
import pytest

from quackflow.connectors.kafka.source import KafkaSource
from quackflow.schema import Schema, String, Timestamp
from quackflow.source import ReplayableSource, Source
from quackflow.time_notion import EventTimeNotion

from .conftest import FakeKafkaConsumer, FakeKafkaMessage

TIMESTAMP_TYPE = 1


def json_deserializer(data: bytes) -> dict[str, Any]:
    obj = json.loads(data.decode("utf-8"))
    if "timestamp" in obj and isinstance(obj["timestamp"], str):
        obj["timestamp"] = dt.datetime.fromisoformat(obj["timestamp"])
    return obj


class TestSchema(Schema):
    id = String()
    timestamp = Timestamp()


class TestProtocolCompliance:
    def test_implements_source_protocol(self):
        consumer = FakeKafkaConsumer()
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            _consumer=consumer,
        )

        assert isinstance(source, Source)

    def test_implements_replayable_source_protocol(self):
        consumer = FakeKafkaConsumer()
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            _consumer=consumer,
        )

        assert isinstance(source, ReplayableSource)


class TestLifecycle:
    def test_watermark_is_none_initially(self):
        consumer = FakeKafkaConsumer()
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            _consumer=consumer,
        )

        assert source.watermark is None

    @pytest.mark.asyncio
    async def test_start_subscribes_to_topic(self):
        consumer = FakeKafkaConsumer()
        source = KafkaSource(
            topic="my-topic",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            _consumer=consumer,
        )

        await source.start()

        assert consumer._subscribed_topics == ["my-topic"]

    @pytest.mark.asyncio
    async def test_stop_closes_consumer(self):
        consumer = FakeKafkaConsumer()
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            _consumer=consumer,
        )

        await source.start()
        await source.stop()

        assert consumer._closed is True


class TestRead:
    @pytest.mark.asyncio
    async def test_read_returns_record_batch(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            deserializer=json_deserializer,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 1

    @pytest.mark.asyncio
    async def test_read_deserializes_json_messages(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "abc", "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            deserializer=json_deserializer,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.to_pydict()["id"] == ["abc"]

    @pytest.mark.asyncio
    async def test_read_returns_empty_batch_on_timeout(self):
        consumer = FakeKafkaConsumer([])
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            poll_timeout=0.01,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.num_rows == 0

    @pytest.mark.asyncio
    async def test_read_batches_up_to_batch_size(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": str(i), "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
            for i in range(10)
        ]
        consumer = FakeKafkaConsumer(messages)
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            batch_size=3,
            deserializer=json_deserializer,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.num_rows == 3


class TestWatermark:
    @pytest.mark.asyncio
    async def test_watermark_updates_after_read(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            deserializer=json_deserializer,
            _consumer=consumer,
        )

        await source.start()
        await source.read()

        assert source.watermark == ts

    @pytest.mark.asyncio
    async def test_watermark_unchanged_on_empty_read(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            deserializer=json_deserializer,
            _consumer=consumer,
        )

        await source.start()
        await source.read()
        await source.read()

        assert source.watermark == ts

    @pytest.mark.asyncio
    async def test_watermark_respects_allowed_lateness(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        lateness = dt.timedelta(seconds=30)
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp", allowed_lateness=lateness),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            deserializer=json_deserializer,
            _consumer=consumer,
        )

        await source.start()
        await source.read()

        assert source.watermark == ts - lateness


class TestSeek:
    @pytest.mark.asyncio
    async def test_seek_uses_offsets_for_times(self):
        consumer = FakeKafkaConsumer()
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            _consumer=consumer,
        )

        await source.start()
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        await source.seek(ts)

        assert consumer._offsets_for_times_called_with is not None


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_deserialization_error_skips_bad_message(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=b"not valid json",
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            ),
            FakeKafkaMessage(
                value=json.dumps({"id": "good", "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            ),
        ]
        consumer = FakeKafkaConsumer(messages)
        source = KafkaSource(
            topic="test",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            deserializer=json_deserializer,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.num_rows == 1
        assert batch.to_pydict()["id"] == ["good"]
