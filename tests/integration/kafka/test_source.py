import json

import pytest

from quackflow import EventTimeNotion
from quackflow.connectors.kafka import KafkaSource

from .conftest import requires_kafka
from .test_data import OrderSchema, make_orders


@pytest.fixture
def produce_orders(create_topic, bootstrap_servers):
    """Produce order messages to a topic."""
    from confluent_kafka import Producer

    def _produce(topic: str, orders: list[dict] | None = None) -> None:
        create_topic(topic)
        producer = Producer({"bootstrap.servers": bootstrap_servers})

        for order in orders or make_orders():
            # Convert datetime to ISO string for JSON serialization
            payload = {**order, "order_time": order["order_time"].isoformat()}
            producer.produce(topic, json.dumps(payload).encode())

        producer.flush()

    return _produce


@requires_kafka
@pytest.mark.integration
@pytest.mark.timeout(30)
class TestKafkaSourceIntegration:
    @pytest.mark.asyncio
    async def test_reads_messages_from_topic(self, unique_topic, unique_group_id, produce_orders, bootstrap_servers):
        produce_orders(unique_topic, make_orders()[:5])

        def deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            import datetime as dt

            record = json.loads(data.decode())
            record["order_time"] = dt.datetime.fromisoformat(record["order_time"])
            return record

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=OrderSchema,
            value_deserializer=deserializer,
            poll_timeout=1.0,
            batch_size=10,
        )

        await source.start()
        batch = await source.read()
        await source.stop()

        assert batch.num_rows == 5

    @pytest.mark.asyncio
    async def test_watermark_advances_after_read(
        self, unique_topic, unique_group_id, produce_orders, bootstrap_servers
    ):
        produce_orders(unique_topic, make_orders()[:3])

        def deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            import datetime as dt

            record = json.loads(data.decode())
            record["order_time"] = dt.datetime.fromisoformat(record["order_time"])
            return record

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=OrderSchema,
            value_deserializer=deserializer,
        )

        assert source.watermark is None

        await source.start()
        await source.read()
        await source.stop()

        assert source.watermark is not None

    @pytest.mark.asyncio
    async def test_respects_batch_size(self, unique_topic, unique_group_id, produce_orders, bootstrap_servers):
        produce_orders(unique_topic, make_orders()[:10])

        def deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            import datetime as dt

            record = json.loads(data.decode())
            record["order_time"] = dt.datetime.fromisoformat(record["order_time"])
            return record

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=OrderSchema,
            value_deserializer=deserializer,
            batch_size=3,
        )

        await source.start()
        batch = await source.read()
        await source.stop()

        assert batch.num_rows == 3

    @pytest.mark.asyncio
    async def test_returns_empty_batch_when_no_messages(
        self, unique_topic, unique_group_id, create_topic, bootstrap_servers
    ):
        create_topic(unique_topic)

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=OrderSchema,
            poll_timeout=0.5,
        )

        await source.start()
        batch = await source.read()
        await source.stop()

        assert batch.num_rows == 0

    @pytest.mark.asyncio
    async def test_deserializes_message_keys(self, unique_topic, unique_group_id, create_topic, bootstrap_servers):
        from confluent_kafka import Producer

        create_topic(unique_topic)
        producer = Producer({"bootstrap.servers": bootstrap_servers})

        order = make_orders()[0]
        key = {"user_id": order["user_id"]}
        payload = {**order, "order_time": order["order_time"].isoformat()}

        producer.produce(
            unique_topic,
            key=json.dumps(key).encode(),
            value=json.dumps(payload).encode(),
        )
        producer.flush()

        def value_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            import datetime as dt

            record = json.loads(data.decode())
            record["order_time"] = dt.datetime.fromisoformat(record["order_time"])
            return record

        def key_deserializer(data: bytes, topic: str, *, is_key: bool = False) -> dict:
            return json.loads(data.decode())

        source = KafkaSource(
            topic=unique_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=unique_group_id,
            schema=OrderSchema,
            value_deserializer=value_deserializer,
            key_deserializer=key_deserializer,
        )

        await source.start()
        batch = await source.read()
        await source.stop()

        assert batch.num_rows == 1
        assert "__key" in batch.schema.names
