import pyarrow as pa
import pytest

from quackflow.schema import Float, Int, Schema, String
from quackflow.sink import Sink

from .conftest import FakeKafkaProducer


class TestSchema(Schema):
    id = String()
    name = String()
    value = Int()


class TestKafkaSinkProtocol:
    def test_implements_sink_protocol(self):
        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            _producer=producer,
        )

        assert isinstance(sink, Sink)


class TestKafkaSinkLifecycle:
    @pytest.mark.asyncio
    async def test_start_does_not_fail(self):
        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            _producer=producer,
        )

        await sink.start()

    @pytest.mark.asyncio
    async def test_stop_flushes_producer(self):
        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            _producer=producer,
        )

        await sink.start()
        await sink.stop()

        assert producer._flushed


class TestKafkaSinkWrite:
    @pytest.mark.asyncio
    async def test_write_produces_messages(self):
        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            _producer=producer,
        )

        batch = pa.RecordBatch.from_pylist(
            [
                {"id": "1", "name": "alice", "value": 100},
                {"id": "2", "name": "bob", "value": 200},
            ]
        )

        await sink.start()
        await sink.write(batch)

        assert len(producer._messages) == 2

    @pytest.mark.asyncio
    async def test_write_uses_json_serializer_by_default(self):
        import json

        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            _producer=producer,
        )

        batch = pa.RecordBatch.from_pylist([{"id": "1", "name": "test", "value": 42}])

        await sink.start()
        await sink.write(batch)

        produced = producer._messages[0]
        assert produced["topic"] == "test-topic"
        assert json.loads(produced["value"].decode("utf-8")) == {
            "id": "1",
            "name": "test",
            "value": 42,
        }

    @pytest.mark.asyncio
    async def test_write_uses_custom_value_serializer(self):
        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()

        def custom_serializer(data: dict, topic: str) -> bytes:
            return f"custom:{data['id']}".encode()

        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            value_serializer=custom_serializer,
            _producer=producer,
        )

        batch = pa.RecordBatch.from_pylist([{"id": "abc", "name": "test", "value": 1}])

        await sink.start()
        await sink.write(batch)

        assert producer._messages[0]["value"] == b"custom:abc"

    @pytest.mark.asyncio
    async def test_write_with_key_serializer(self):
        import json

        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()

        def key_serializer(data: dict, topic: str) -> bytes:
            return json.dumps(data).encode("utf-8")

        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            key_serializer=key_serializer,
            _producer=producer,
        )

        batch = pa.RecordBatch.from_pylist(
            [
                {"id": "1", "name": "alice", "value": 100, "__key": {"partition_key": "A"}},
            ]
        )

        await sink.start()
        await sink.write(batch)

        key_bytes = producer._messages[0]["key"]
        assert json.loads(key_bytes.decode("utf-8")) == {"partition_key": "A"}

    @pytest.mark.asyncio
    async def test_write_without_key_when_no_key_serializer(self):
        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            _producer=producer,
        )

        batch = pa.RecordBatch.from_pylist([{"id": "1", "name": "test", "value": 42}])

        await sink.start()
        await sink.write(batch)

        assert producer._messages[0]["key"] is None

    @pytest.mark.asyncio
    async def test_write_empty_batch(self):
        from quackflow.connectors.kafka.sink import KafkaSink

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="test-topic",
            bootstrap_servers="localhost:9092",
            _producer=producer,
        )

        batch = pa.RecordBatch.from_pylist([])

        await sink.start()
        await sink.write(batch)

        assert len(producer._messages) == 0


class TestKafkaSinkAvroSerialization:
    @pytest.mark.asyncio
    async def test_avro_serializer_integration(self):
        from quackflow.connectors.kafka.deserializers import ConfluentAvroDeserializer
        from quackflow.connectors.kafka.serializers import ConfluentAvroSerializer
        from quackflow.connectors.kafka.sink import KafkaSink

        from .conftest import FakeSchemaRegistryClient

        class EventSchema(Schema):
            id = String()
            count = Int()
            score = Float()

        sr = FakeSchemaRegistryClient()
        serializer = ConfluentAvroSerializer(sr, schema=EventSchema)
        deserializer = ConfluentAvroDeserializer(sr)

        producer = FakeKafkaProducer()
        sink = KafkaSink(
            topic="events",
            bootstrap_servers="localhost:9092",
            value_serializer=serializer,
            _producer=producer,
        )

        batch = pa.RecordBatch.from_pylist(
            [
                {"id": "abc", "count": 42, "score": 3.14},
            ]
        )

        await sink.start()
        await sink.write(batch)

        serialized = producer._messages[0]["value"]
        deserialized = deserializer(serialized, "events")

        assert deserialized == {"id": "abc", "count": 42, "score": 3.14}
