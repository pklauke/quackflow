import datetime as dt
import json
from typing import Any

import pyarrow as pa
import pytest

from quackflow.connectors.kafka.source import KafkaSource
from quackflow.schema import Schema, String, Timestamp
from quackflow.source import ReplayableSource, Source
from quackflow.time_notion import EventTimeNotion, ProcessingTimeNotion

from .conftest import FakeKafkaConsumer, FakeKafkaMessage

TIMESTAMP_TYPE = 1


def value_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict[str, Any]:
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
            value_deserializer=value_deserializer,
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
            value_deserializer=value_deserializer,
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
            value_deserializer=value_deserializer,
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
            value_deserializer=value_deserializer,
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
            value_deserializer=value_deserializer,
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
            value_deserializer=value_deserializer,
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
            value_deserializer=value_deserializer,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.num_rows == 1
        assert batch.to_pydict()["id"] == ["good"]


class TestDeserializers:
    @pytest.mark.asyncio
    async def test_value_deserializer_receives_topic(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        received_topics: list[str] = []

        def tracking_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict[str, Any]:
            received_topics.append(topic)
            return value_deserializer(data, topic)

        source = KafkaSource(
            topic="my-topic",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            value_deserializer=tracking_deserializer,
            _consumer=consumer,
        )

        await source.start()
        await source.read()

        assert received_topics == ["my-topic"]

    @pytest.mark.asyncio
    async def test_key_deserializer_adds_key_to_record(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                key=json.dumps({"user_id": "u123", "region": "us"}).encode(),
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
            value_deserializer=value_deserializer,
            key_deserializer=lambda b, _, is_key=False: json.loads(b.decode("utf-8")),
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.to_pydict()["__key"] == [{"user_id": "u123", "region": "us"}]
        assert batch.to_pydict()["id"] == ["1"]

    @pytest.mark.asyncio
    async def test_key_deserializer_receives_topic(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                key=json.dumps({"user_id": "u123"}).encode(),
                timestamp=(TIMESTAMP_TYPE, int(ts.timestamp() * 1000)),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        received_topics: list[str] = []

        def tracking_key_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict[str, Any]:
            received_topics.append(topic)
            return json.loads(data.decode("utf-8"))

        source = KafkaSource(
            topic="my-topic",
            time_notion=EventTimeNotion(column="timestamp"),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            value_deserializer=value_deserializer,
            key_deserializer=tracking_key_deserializer,
            _consumer=consumer,
        )

        await source.start()
        await source.read()

        assert received_topics == ["my-topic"]

    @pytest.mark.asyncio
    async def test_no_key_when_no_key_deserializer(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                key=b"some-key",
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
            value_deserializer=value_deserializer,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert "__key" not in batch.to_pydict()

    @pytest.mark.asyncio
    async def test_default_json_value_deserializer(self):
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "name": "test"}).encode(),
                timestamp=(TIMESTAMP_TYPE, 1704110400000),
            )
        ]
        consumer = FakeKafkaConsumer(messages)
        source = KafkaSource(
            topic="test",
            time_notion=ProcessingTimeNotion(),
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            schema=TestSchema,
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.to_pydict()["id"] == ["1"]

    @pytest.mark.asyncio
    async def test_null_key_skipped(self):
        ts = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        messages = [
            FakeKafkaMessage(
                value=json.dumps({"id": "1", "timestamp": ts.isoformat()}).encode(),
                key=None,
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
            value_deserializer=value_deserializer,
            key_deserializer=lambda b, _, is_key=False: json.loads(b.decode("utf-8")),
            _consumer=consumer,
        )

        await source.start()
        batch = await source.read()

        assert batch.to_pydict().get("__key") in [None, [None]]
