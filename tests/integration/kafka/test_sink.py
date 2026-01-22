import json

import pyarrow as pa
import pytest

from quackflow.connectors.kafka import KafkaSink

from .conftest import requires_kafka
from .test_data import make_orders


@requires_kafka
@pytest.mark.integration
@pytest.mark.timeout(30)
class TestKafkaSinkIntegration:
    @pytest.mark.asyncio
    async def test_writes_messages_to_topic(self, unique_topic, create_topic, bootstrap_servers):
        from confluent_kafka import Consumer

        create_topic(unique_topic)

        orders = make_orders()[:5]
        rows = [{**o, "order_time": o["order_time"].isoformat()} for o in orders]
        batch = pa.RecordBatch.from_pylist(rows)

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        # Verify messages were written
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "verify-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([unique_topic])

        messages = []
        for _ in range(10):
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if not msg.error():
                messages.append(json.loads(msg.value().decode()))

        consumer.close()

        assert len(messages) == 5

    @pytest.mark.asyncio
    async def test_writes_messages_with_keys(self, unique_topic, create_topic, bootstrap_servers):
        from confluent_kafka import Consumer

        create_topic(unique_topic)

        orders = make_orders()[:3]
        rows = [{**o, "order_time": o["order_time"].isoformat(), "__key": {"user_id": o["user_id"]}} for o in orders]
        batch = pa.RecordBatch.from_pylist(rows)

        def key_serializer(data: dict, topic: str, *, is_key: bool = False) -> bytes:
            return json.dumps(data).encode()

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
            key_serializer=key_serializer,
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        # Verify messages and keys were written
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "verify-keys-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([unique_topic])

        messages = []
        for _ in range(10):
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if not msg.error():
                messages.append(
                    {
                        "key": json.loads(msg.key().decode()) if msg.key() else None,
                        "value": json.loads(msg.value().decode()),
                    }
                )

        consumer.close()

        assert len(messages) == 3
        assert all(m["key"] is not None for m in messages)
        assert messages[0]["key"]["user_id"] == "u-1"

    @pytest.mark.asyncio
    async def test_handles_empty_batch(self, unique_topic, create_topic, bootstrap_servers):
        from confluent_kafka import Consumer

        create_topic(unique_topic)

        batch = pa.RecordBatch.from_pylist([])

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
        )

        await sink.start()
        await sink.write(batch)
        await sink.stop()

        # Verify no messages were written
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "verify-empty-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([unique_topic])

        msg = consumer.poll(1.0)
        consumer.close()

        assert msg is None or msg.error() is not None

    @pytest.mark.asyncio
    async def test_writes_multiple_batches(self, unique_topic, create_topic, bootstrap_servers):
        from confluent_kafka import Consumer

        create_topic(unique_topic)

        orders = make_orders()
        batch1 = pa.RecordBatch.from_pylist([{**o, "order_time": o["order_time"].isoformat()} for o in orders[:5]])
        batch2 = pa.RecordBatch.from_pylist([{**o, "order_time": o["order_time"].isoformat()} for o in orders[5:10]])

        sink = KafkaSink(
            topic=unique_topic,
            bootstrap_servers=bootstrap_servers,
        )

        await sink.start()
        await sink.write(batch1)
        await sink.write(batch2)
        await sink.stop()

        # Verify all messages were written
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "verify-multi-group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([unique_topic])

        messages = []
        for _ in range(15):
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if not msg.error():
                messages.append(json.loads(msg.value().decode()))

        consumer.close()

        assert len(messages) == 10
