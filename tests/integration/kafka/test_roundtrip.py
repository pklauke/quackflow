import datetime as dt

import pyarrow as pa
import pytest

from quackflow import EventTimeNotion
from quackflow.connectors.kafka import KafkaSink, KafkaSource

from .conftest import requires_kafka
from .test_data import OrderSchema, make_orders


class TimestampAwareJsonSerializer:
    """JSON serializer that converts datetime to ISO strings."""

    def __call__(self, data: dict, topic: str, *, is_key: bool = False) -> bytes:
        import json

        payload = {k: v.isoformat() if isinstance(v, dt.datetime) else v for k, v in data.items()}
        return json.dumps(payload).encode()


class TimestampAwareJsonDeserializer:
    """JSON deserializer that parses ISO strings back to datetime."""

    def __call__(self, data: bytes, topic: str, *, is_key: bool = False) -> dict:
        import json

        record = json.loads(data.decode())
        if "order_time" in record:
            record["order_time"] = dt.datetime.fromisoformat(record["order_time"])
        if "delivery_time" in record:
            record["delivery_time"] = dt.datetime.fromisoformat(record["delivery_time"])
        return record


async def read_until(source: KafkaSource, min_rows: int, max_attempts: int = 10) -> pa.RecordBatch:
    """Read from source until min_rows are collected or max_attempts reached."""
    all_rows: list[dict] = []
    for _ in range(max_attempts):
        batch = await source.read()
        all_rows.extend(batch.to_pylist())
        if len(all_rows) >= min_rows:
            break
    return pa.RecordBatch.from_pylist(all_rows) if all_rows else batch


@requires_kafka
@pytest.mark.integration
@pytest.mark.timeout(30)
class TestKafkaRoundTrip:
    @pytest.mark.asyncio
    async def test_sink_to_source_roundtrip(self, unique_topic, unique_group_id, create_topic, bootstrap_servers):
        """Write with KafkaSink, read with KafkaSource, verify data integrity."""
        create_topic(unique_topic)

        original_orders = make_orders()[:5]
        batch = pa.RecordBatch.from_pylist(original_orders)

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=TimestampAwareJsonSerializer(),
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=OrderSchema,
            value_deserializer=TimestampAwareJsonDeserializer(),
            batch_size=10,
        )

        await source.start()
        read_batch = await read_until(source, min_rows=5)
        await source.stop()

        assert read_batch.num_rows == 5

        read_orders = read_batch.to_pylist()
        for original, read in zip(original_orders, read_orders, strict=True):
            assert read["order_id"] == original["order_id"]
            assert read["user_id"] == original["user_id"]
            assert read["amount"] == original["amount"]
            assert read["region"] == original["region"]

    @pytest.mark.asyncio
    async def test_roundtrip_with_keys(self, unique_topic, unique_group_id, create_topic, bootstrap_servers):
        """Round-trip with message keys preserved."""
        import json

        create_topic(unique_topic)

        original_orders = make_orders()[:3]
        rows = [{**o, "__key": {"user_id": o["user_id"]}} for o in original_orders]
        batch = pa.RecordBatch.from_pylist(rows)

        def key_serializer(data: dict, topic: str, *, is_key: bool = False) -> bytes:
            return json.dumps(data).encode()

        def key_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            return json.loads(data.decode())

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=TimestampAwareJsonSerializer(),
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
            schema=OrderSchema,
            value_deserializer=TimestampAwareJsonDeserializer(),
            key_deserializer=key_deserializer,
            batch_size=10,
        )

        await source.start()
        read_batch = await read_until(source, min_rows=3)
        await source.stop()

        assert read_batch.num_rows == 3
        assert "__key" in read_batch.schema.names

        read_orders = read_batch.to_pylist()
        for original, read in zip(original_orders, read_orders, strict=True):
            assert read["__key"]["user_id"] == original["user_id"]

    @pytest.mark.asyncio
    async def test_roundtrip_preserves_data_types(self, unique_topic, unique_group_id, create_topic, bootstrap_servers):
        """Verify all field types are preserved through serialization round-trip."""
        create_topic(unique_topic)

        original_order = make_orders()[0]
        batch = pa.RecordBatch.from_pylist([original_order])

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=TimestampAwareJsonSerializer(),
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=OrderSchema,
            value_deserializer=TimestampAwareJsonDeserializer(),
        )

        await source.start()
        read_batch = await read_until(source, min_rows=1)
        await source.stop()

        read_order = read_batch.to_pylist()[0]

        # String fields
        assert isinstance(read_order["order_id"], str)
        assert read_order["order_id"] == original_order["order_id"]

        # Float field
        assert isinstance(read_order["amount"], float)
        assert read_order["amount"] == original_order["amount"]

        # Timestamp field
        assert isinstance(read_order["order_time"], dt.datetime)
        assert read_order["order_time"] == original_order["order_time"]
