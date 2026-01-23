#!/usr/bin/env python3
"""Generate and produce test data to Kafka topics."""

import argparse
import asyncio
import datetime as dt
import heapq
import random
import uuid
from collections.abc import Iterator
from typing import Any

from confluent_kafka import Producer

from schemas import DeliverySchema, OrderSchema
from utils import TimestampAwareJsonSerializer

REGIONS = ["us-east", "us-west", "eu-west", "ap-south"]
REGION_WEIGHTS = [0.35, 0.30, 0.25, 0.10]


def hour_multiplier(hour: int) -> float:
    """Get order rate multiplier based on hour of day."""
    if 9 <= hour < 18:
        return 1.5
    elif 0 <= hour < 6:
        return 0.3
    else:
        return 1.0


def generate_orders(start: dt.datetime, end: dt.datetime, orders_per_hour: int) -> Iterator[dict[str, Any]]:
    """Generate orders distributed across time range."""
    current = start.replace(minute=0, second=0, microsecond=0)

    while current < end:
        hour = current.hour
        multiplier = hour_multiplier(hour)
        num_orders = int(orders_per_hour * multiplier * random.uniform(0.9, 1.1))

        for _ in range(num_orders):
            order_time = current + dt.timedelta(seconds=random.randint(0, 3599))
            if order_time >= end:
                continue

            yield {
                "order_id": f"ord-{uuid.uuid4().hex[:12]}",
                "user_id": f"user-{random.randint(1, 10000):05d}",
                "product_id": f"prod-{random.randint(1, 100):03d}",
                "amount": round(random.uniform(5.0, 500.0), 2),
                "region": random.choices(REGIONS, weights=REGION_WEIGHTS)[0],
                "order_time": order_time,
            }

        current += dt.timedelta(hours=1)


def generate_delivery(order: dict[str, Any], avg_lag_minutes: int) -> dict[str, Any]:
    """Generate delivery for an order."""
    lag_minutes = random.gauss(avg_lag_minutes, avg_lag_minutes / 3)
    lag_minutes = max(10, min(120, lag_minutes))

    return {
        "delivery_id": f"del-{uuid.uuid4().hex[:12]}",
        "order_id": order["order_id"],
        "courier_id": f"courier-{random.randint(1, 50):03d}",
        "delivery_time": order["order_time"] + dt.timedelta(minutes=lag_minutes),
    }


def produce_backfill(
    start: dt.datetime,
    end: dt.datetime,
    orders_per_hour: int,
    delivery_rate: float,
    delivery_lag_minutes: int,
    producer: Producer,
    order_serializer: Any,
    delivery_serializer: Any,
    orders_topic: str,
    deliveries_topic: str,
) -> tuple[int, int]:
    """Produce historical data."""
    total_orders = 0
    total_deliveries = 0

    current_hour = start.replace(minute=0, second=0, microsecond=0)
    hour_num = 0
    total_hours = int((end - start).total_seconds() / 3600) + 1

    while current_hour < end:
        hour_end = current_hour + dt.timedelta(hours=1)
        if hour_end > end:
            hour_end = end

        messages: list[tuple[dt.datetime, str, bytes]] = []

        for order in generate_orders(current_hour, hour_end, orders_per_hour):
            order_bytes = order_serializer(order, orders_topic)
            messages.append((order["order_time"], orders_topic, order_bytes))
            total_orders += 1

            if random.random() < delivery_rate:
                delivery = generate_delivery(order, delivery_lag_minutes)
                delivery_bytes = delivery_serializer(delivery, deliveries_topic)
                messages.append((delivery["delivery_time"], deliveries_topic, delivery_bytes))
                total_deliveries += 1

        messages.sort(key=lambda x: x[0])

        for _, topic, data in messages:
            while True:
                try:
                    producer.produce(topic, value=data)
                    producer.poll(0)
                    break
                except BufferError:
                    producer.poll(1)

        producer.flush()

        hour_num += 1
        hour_orders = sum(1 for m in messages if m[1] == orders_topic)
        hour_deliveries = sum(1 for m in messages if m[1] == deliveries_topic)
        print(
            f"[{current_hour.strftime('%Y-%m-%d %H:%M')}] Hour {hour_num:>2}/{total_hours}: "
            f"{hour_orders:,} orders, {hour_deliveries:,} deliveries "
            f"(cumulative: {total_orders:,} / {total_deliveries:,})"
        )

        current_hour = hour_end

    return total_orders, total_deliveries


async def produce_continuous(
    orders_per_hour: int,
    delivery_rate: float,
    delivery_lag_minutes: int,
    producer: Producer,
    order_serializer: Any,
    delivery_serializer: Any,
    orders_topic: str,
    deliveries_topic: str,
) -> None:
    """Produce data continuously in real-time."""
    orders_per_second = orders_per_hour / 3600
    sleep_time = 1.0 / orders_per_second if orders_per_second > 0 else 1.0

    pending_deliveries: list[tuple[dt.datetime, dict[str, Any]]] = []
    total_orders = 0
    total_deliveries = 0
    last_report = dt.datetime.now(dt.timezone.utc)

    print("\nContinuous mode started. Press Ctrl+C to stop.\n")

    while True:
        now = dt.datetime.now(dt.timezone.utc)

        order = {
            "order_id": f"ord-{uuid.uuid4().hex[:12]}",
            "user_id": f"user-{random.randint(1, 10000):05d}",
            "product_id": f"prod-{random.randint(1, 100):03d}",
            "amount": round(random.uniform(5.0, 500.0), 2),
            "region": random.choices(REGIONS, weights=REGION_WEIGHTS)[0],
            "order_time": now,
        }

        order_bytes = order_serializer(order, orders_topic)
        producer.produce(orders_topic, value=order_bytes)
        producer.poll(0)
        total_orders += 1

        if random.random() < delivery_rate:
            delivery = generate_delivery(order, delivery_lag_minutes)
            heapq.heappush(pending_deliveries, (delivery["delivery_time"], delivery))

        while pending_deliveries and pending_deliveries[0][0] <= now:
            _, delivery = heapq.heappop(pending_deliveries)
            delivery_bytes = delivery_serializer(delivery, deliveries_topic)
            producer.produce(deliveries_topic, value=delivery_bytes)
            producer.poll(0)
            total_deliveries += 1

        if (now - last_report).total_seconds() >= 60:
            elapsed = (now - last_report).total_seconds()
            rate = total_orders / max(1, elapsed) if last_report else 0
            print(
                f"[{now.strftime('%H:%M:%S')}] Rate: {rate:.1f}/s | "
                f"Orders: {total_orders:,} | Deliveries: {total_deliveries:,} | "
                f"Pending: {len(pending_deliveries)}"
            )
            last_report = now

        await asyncio.sleep(sleep_time * random.uniform(0.8, 1.2))


def create_topics_if_needed(bootstrap_servers: str, topics: list[str]) -> None:
    """Create topics if they don't exist."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in existing]

    if to_create:
        futures = admin.create_topics(to_create)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"Created topic: {topic}")
            except Exception as e:
                if "already exists" not in str(e):
                    print(f"Warning: Failed to create topic {topic}: {e}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and produce test data to Kafka topics")

    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--schema-registry", default="http://localhost:8081", help="Schema Registry URL")
    parser.add_argument(
        "--start-date",
        type=lambda s: dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc),
        default=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24),
        help="Start of data range, ISO format (default: 24h ago)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc),
        default=dt.datetime.now(dt.timezone.utc),
        help="End of data range, ISO format (default: now)",
    )
    parser.add_argument("--orders-per-hour", type=int, default=5000, help="Average orders per hour")
    parser.add_argument("--delivery-rate", type=float, default=0.85, help="Fraction of orders with delivery")
    parser.add_argument("--delivery-lag-minutes", type=int, default=30, help="Average delivery lag in minutes")
    parser.add_argument("--continuous", action="store_true", help="Continue producing in real-time after backfill")
    parser.add_argument("--orders-topic", default="orders", help="Orders topic name")
    parser.add_argument("--deliveries-topic", default="deliveries", help="Deliveries topic name")
    parser.add_argument("--serialization", choices=["json", "avro"], default="json", help="Serialization format")

    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    create_topics_if_needed(args.bootstrap_servers, [args.orders_topic, args.deliveries_topic])

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    if args.serialization == "avro":
        from confluent_kafka.schema_registry import SchemaRegistryClient

        from quackflow.connectors.kafka import ConfluentAvroSerializer

        sr = SchemaRegistryClient({"url": args.schema_registry})
        order_serializer = ConfluentAvroSerializer(sr, schema=OrderSchema)
        delivery_serializer = ConfluentAvroSerializer(sr, schema=DeliverySchema)
    else:
        order_serializer = TimestampAwareJsonSerializer()
        delivery_serializer = TimestampAwareJsonSerializer()

    print(f"Backfilling {args.start_date.isoformat()} to {args.end_date.isoformat()}...")

    orders, deliveries = produce_backfill(
        start=args.start_date,
        end=args.end_date,
        orders_per_hour=args.orders_per_hour,
        delivery_rate=args.delivery_rate,
        delivery_lag_minutes=args.delivery_lag_minutes,
        producer=producer,
        order_serializer=order_serializer,
        delivery_serializer=delivery_serializer,
        orders_topic=args.orders_topic,
        deliveries_topic=args.deliveries_topic,
    )

    print(f"\nBackfill complete: {orders:,} orders, {deliveries:,} deliveries")

    if args.continuous:
        print("\nSwitching to continuous mode (Ctrl+C to stop)...")
        try:
            await produce_continuous(
                orders_per_hour=args.orders_per_hour,
                delivery_rate=args.delivery_rate,
                delivery_lag_minutes=args.delivery_lag_minutes,
                producer=producer,
                order_serializer=order_serializer,
                delivery_serializer=delivery_serializer,
                orders_topic=args.orders_topic,
                deliveries_topic=args.deliveries_topic,
            )
        except KeyboardInterrupt:
            print("\nStopping...")

    producer.flush()


if __name__ == "__main__":
    asyncio.run(main())
