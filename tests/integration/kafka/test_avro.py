import datetime as dt

import pyarrow as pa
import pytest

from quackflow import EventTimeNotion
from quackflow.connectors.kafka import (
    ConfluentAvroDeserializer,
    ConfluentAvroSerializer,
    KafkaSink,
    KafkaSource,
)
from quackflow.schema import Float, Schema, String, Timestamp

from .conftest import requires_kafka, requires_schema_registry
from .test_data import BASE_DATE


async def read_until(source: KafkaSource, min_rows: int, max_attempts: int = 10) -> pa.RecordBatch:
    """Read from source until min_rows are collected or max_attempts reached."""
    all_rows: list[dict] = []
    for _ in range(max_attempts):
        batch = await source.read()
        all_rows.extend(batch.to_pylist())
        if len(all_rows) >= min_rows:
            break
    return pa.RecordBatch.from_pylist(all_rows) if all_rows else batch


class AvroOrderSchema(Schema):
    """Order schema for Avro serialization (no Timestamp - use long micros)."""

    order_id = String()
    user_id = String()
    product_id = String()
    amount = Float()
    region = String()
    order_time = Timestamp()


def make_avro_orders() -> list[dict]:
    """Generate orders with timestamps as microseconds for Avro."""
    return [
        {
            "order_id": "o-001",
            "user_id": "u-1",
            "product_id": "p-10",
            "amount": 29.99,
            "region": "us-east",
            "order_time": BASE_DATE,
        },
        {
            "order_id": "o-002",
            "user_id": "u-2",
            "product_id": "p-20",
            "amount": 49.99,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=3),
        },
        {
            "order_id": "o-003",
            "user_id": "u-1",
            "product_id": "p-30",
            "amount": 15.00,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=7),
        },
    ]


@requires_kafka
@requires_schema_registry
@pytest.mark.integration
@pytest.mark.timeout(30)
class TestAvroIntegration:
    @pytest.mark.asyncio
    async def test_avro_sink_to_source_roundtrip(
        self, unique_topic, unique_group_id, create_topic, bootstrap_servers, schema_registry_client
    ):
        """Write Avro with KafkaSink, read with KafkaSource."""
        create_topic(unique_topic)

        original_orders = make_avro_orders()
        batch = pa.RecordBatch.from_pylist(original_orders)

        serializer = ConfluentAvroSerializer(schema_registry_client, schema=AvroOrderSchema)
        deserializer = ConfluentAvroDeserializer(schema_registry_client)

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=serializer,
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=AvroOrderSchema,
            value_deserializer=deserializer,
            batch_size=10,
        )

        await source.start()
        read_batch = await read_until(source, min_rows=3)
        await source.stop()

        assert read_batch.num_rows == 3

        read_orders = read_batch.to_pylist()
        for original, read in zip(original_orders, read_orders, strict=True):
            assert read["order_id"] == original["order_id"]
            assert read["user_id"] == original["user_id"]
            assert read["amount"] == original["amount"]
            assert read["region"] == original["region"]

    @pytest.mark.asyncio
    async def test_schema_registered_in_registry(
        self, unique_topic, create_topic, bootstrap_servers, schema_registry_client
    ):
        """Verify schema is registered after producing."""
        create_topic(unique_topic)

        orders = make_avro_orders()[:1]
        batch = pa.RecordBatch.from_pylist(orders)

        serializer = ConfluentAvroSerializer(schema_registry_client, schema=AvroOrderSchema)

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=serializer,
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        # Verify schema was registered
        subjects = schema_registry_client.get_subjects()
        assert f"{unique_topic}-value" in subjects

    @pytest.mark.asyncio
    async def test_avro_with_keys(
        self, unique_topic, unique_group_id, create_topic, bootstrap_servers, schema_registry_client
    ):
        """Test Avro serialization with message keys."""
        import json

        create_topic(unique_topic)

        original_orders = make_avro_orders()[:2]
        rows = [{**o, "__key": {"user_id": o["user_id"]}} for o in original_orders]
        batch = pa.RecordBatch.from_pylist(rows)

        def key_serializer(data: dict, topic: str, *, is_key: bool = False) -> bytes:
            return json.dumps(data).encode()

        def key_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            return json.loads(data.decode())

        serializer = ConfluentAvroSerializer(schema_registry_client, schema=AvroOrderSchema)
        deserializer = ConfluentAvroDeserializer(schema_registry_client)

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=serializer,
            key_serializer=key_serializer,
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=AvroOrderSchema,
            value_deserializer=deserializer,
            key_deserializer=key_deserializer,
            batch_size=10,
        )

        await source.start()
        read_batch = await read_until(source, min_rows=2)
        await source.stop()

        assert read_batch.num_rows == 2
        assert "__key" in read_batch.schema.names

        read_orders = read_batch.to_pylist()
        assert read_orders[0]["__key"]["user_id"] == "u-1"
        assert read_orders[1]["__key"]["user_id"] == "u-2"


@requires_kafka
@requires_schema_registry
@pytest.mark.integration
@pytest.mark.timeout(30)
class TestMixedSerialization:
    @pytest.mark.asyncio
    async def test_avro_values_json_keys(
        self, unique_topic, unique_group_id, create_topic, bootstrap_servers, schema_registry_client
    ):
        """Test Avro value serialization with JSON key serialization."""
        import json

        create_topic(unique_topic)

        orders = make_avro_orders()[:2]
        rows = [{**o, "__key": {"user_id": o["user_id"]}} for o in orders]
        batch = pa.RecordBatch.from_pylist(rows)

        def json_key_serializer(data: dict, topic: str, *, is_key: bool = False) -> bytes:
            return json.dumps(data).encode()

        def json_key_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            return json.loads(data.decode())

        avro_serializer = ConfluentAvroSerializer(schema_registry_client, schema=AvroOrderSchema)
        avro_deserializer = ConfluentAvroDeserializer(schema_registry_client)

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=avro_serializer,
            key_serializer=json_key_serializer,
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=AvroOrderSchema,
            value_deserializer=avro_deserializer,
            key_deserializer=json_key_deserializer,
            batch_size=10,
        )

        await source.start()
        read_batch = await read_until(source, min_rows=2)
        await source.stop()

        assert read_batch.num_rows == 2
        read_orders = read_batch.to_pylist()
        assert read_orders[0]["order_id"] == "o-001"
        assert read_orders[0]["__key"]["user_id"] == "u-1"
