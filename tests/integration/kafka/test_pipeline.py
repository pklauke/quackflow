import datetime as dt

import pyarrow as pa
import pytest

from quackflow import EventTimeNotion, Quackflow
from quackflow.connectors.kafka import KafkaSink, KafkaSource
from quackflow.runtime import Runtime
from quackflow.schema import Float, Int, Schema, String, Timestamp

from .conftest import requires_kafka
from .test_data import BASE_DATE


class OrderSchema(Schema):
    order_id = String()
    user_id = String()
    amount = Float()
    region = String()
    order_time = Timestamp()


class RegionalRevenueSchema(Schema):
    window_start = Timestamp()
    window_end = Timestamp()
    region = String()
    total_revenue = Float()
    num_orders = Int()


class TimestampAwareJsonSerializer:
    def __call__(self, data: dict, topic: str, *, is_key: bool = False) -> bytes:
        import json

        payload = {k: v.isoformat() if isinstance(v, dt.datetime) else v for k, v in data.items()}
        return json.dumps(payload).encode()


class TimestampAwareJsonDeserializer:
    def __call__(self, data: bytes, topic: str, *, is_key: bool = False) -> dict:
        import json

        record = json.loads(data.decode())
        for key in ["order_time", "window_start", "window_end"]:
            if key in record and record[key] is not None:
                record[key] = dt.datetime.fromisoformat(record[key])
        return record


def make_test_orders() -> list[dict]:
    """Generate orders for pipeline test.

    Creates 7 orders across 2 regions over 10 minutes:
    - us-east: 3 orders totaling 145.00 (30 + 15 + 100)
    - us-west: 4 orders totaling 130.01 (50 + 30 + 50 + 0.01)

    The final order at minute 10 ensures the 10:05-10:10 window triggers.
    """
    return [
        {
            "order_id": "o-001",
            "user_id": "u-1",
            "amount": 30.0,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=1),
        },
        {
            "order_id": "o-002",
            "user_id": "u-2",
            "amount": 50.0,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=2),
        },
        {
            "order_id": "o-003",
            "user_id": "u-1",
            "amount": 15.0,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=3),
        },
        {
            "order_id": "o-004",
            "user_id": "u-3",
            "amount": 30.0,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=6),
        },
        {
            "order_id": "o-005",
            "user_id": "u-2",
            "amount": 100.0,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=7),
        },
        {
            "order_id": "o-006",
            "user_id": "u-4",
            "amount": 50.0,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=9),
        },
        {
            "order_id": "o-007",
            "user_id": "u-1",
            "amount": 0.01,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=10),
        },
    ]


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
@pytest.mark.timeout(60)
class TestStreamAggregationPipeline:
    @pytest.mark.asyncio
    async def test_regional_revenue_aggregation(
        self,
        unique_topic,
        unique_group_id,
        create_topic,
        bootstrap_servers,
    ):
        """End-to-end test: Kafka -> Aggregation Pipeline -> Kafka.

        1. Write orders to input Kafka topic
        2. Run pipeline that aggregates revenue by region in 10-minute windows
        3. Read aggregated results from output Kafka topic
        4. Verify aggregation correctness
        """
        input_topic = f"{unique_topic}-orders"
        output_topic = f"{unique_topic}-revenue"
        create_topic(input_topic)
        create_topic(output_topic)

        # Step 1: Write orders to input topic
        orders = make_test_orders()
        input_batch = pa.RecordBatch.from_pylist(orders)

        input_sink = KafkaSink(
            topic=input_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=TimestampAwareJsonSerializer(),
        )
        await input_sink.start()
        await input_sink.write(input_batch)
        await input_sink.stop()

        # Step 2: Build and run the aggregation pipeline
        app = Quackflow()
        app.source("orders", schema=OrderSchema)

        app.output(
            "regional_revenue",
            """
            SELECT
                window_start,
                window_end,
                region,
                SUM(amount) AS total_revenue,
                COUNT(*) AS num_orders
            FROM HOP('orders', 'order_time', INTERVAL '5 minutes')
            GROUP BY window_start, window_end, region
            """,
            schema=RegionalRevenueSchema,
        ).trigger(window=dt.timedelta(minutes=5))

        pipeline_source = KafkaSource(
            topic=input_topic,
            time_notion=EventTimeNotion("order_time"),
            bootstrap_servers=bootstrap_servers,
            group_id=f"{unique_group_id}-pipeline",
            schema=OrderSchema,
            value_deserializer=TimestampAwareJsonDeserializer(),
            batch_size=10,
        )

        pipeline_sink = KafkaSink(
            topic=output_topic,
            bootstrap_servers=bootstrap_servers,
            value_serializer=TimestampAwareJsonSerializer(),
        )

        runtime = Runtime(
            app,
            sources={"orders": pipeline_source},
            sinks={"regional_revenue": pipeline_sink},
        )

        # Execute pipeline for the time range covering our test data
        # BASE_DATE is 2024-01-15 10:00:00, orders span 10:01 to 10:10
        await runtime.execute(
            start=BASE_DATE,
            end=BASE_DATE + dt.timedelta(minutes=10),
        )

        # Step 3: Read results from output topic
        result_source = KafkaSource(
            topic=output_topic,
            time_notion=EventTimeNotion("window_end"),
            bootstrap_servers=bootstrap_servers,
            group_id=f"{unique_group_id}-results",
            schema=RegionalRevenueSchema,
            value_deserializer=TimestampAwareJsonDeserializer(),
            batch_size=10,
        )

        await result_source.start()
        result_batch = await read_until(result_source, min_rows=2)
        await result_source.stop()

        # Step 4: Verify results
        results = result_batch.to_pylist()

        # Group results by region
        by_region: dict[str, list[dict]] = {}
        for row in results:
            region = row["region"]
            if region not in by_region:
                by_region[region] = []
            by_region[region].append(row)

        # Verify us-east: 3 orders, total 145.00
        assert "us-east" in by_region
        us_east_total = sum(r["total_revenue"] for r in by_region["us-east"])
        us_east_count = sum(r["num_orders"] for r in by_region["us-east"])
        assert us_east_total == 145.0
        assert us_east_count == 3

        # Verify us-west: 3 orders in [10:00,10:10), total 130.00
        # The order at 10:10 is in window [10:10,10:15) which doesn't fire yet
        assert "us-west" in by_region
        us_west_total = sum(r["total_revenue"] for r in by_region["us-west"])
        us_west_count = sum(r["num_orders"] for r in by_region["us-west"])
        assert us_west_total == 130.0
        assert us_west_count == 3
