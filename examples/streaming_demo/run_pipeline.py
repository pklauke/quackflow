#!/usr/bin/env python3
"""Run the streaming pipeline and display progress."""

import argparse
import asyncio
import contextlib
import datetime as dt
import logging
from datetime import timedelta

import pyarrow as pa

from quackflow import EventTimeNotion, Quackflow
from quackflow.connectors.kafka import KafkaSink, KafkaSource
from quackflow.runtime import Runtime

from schemas import DeliverySchema, OrderSchema, RevenueByRegionSchema
from utils import TimestampAwareJsonDeserializer, TimestampAwareJsonSerializer, parse_duration


def build_pipeline(window: timedelta, trigger: timedelta) -> Quackflow:
    """Build the streaming pipeline."""
    app = Quackflow()

    app.source("orders", schema=OrderSchema, ts_col="order_time")
    app.source("deliveries", schema=DeliverySchema, ts_col="delivery_time")

    app.view(
        "fulfilled",
        """
        SELECT
            o.order_id,
            o.user_id,
            o.amount,
            o.region,
            o.order_time,
            d.delivery_time,
            EXTRACT(EPOCH FROM (d.delivery_time - o.order_time)) / 60.0 AS delivery_minutes
        FROM orders o
        JOIN deliveries d ON o.order_id = d.order_id
        """,
    )

    window_seconds = int(window.total_seconds())
    app.output(
        "revenue_by_region",
        f"""
        SELECT
            region,
            SUM(amount) AS total_revenue,
            COUNT(*) AS num_orders,
            AVG(delivery_minutes) AS avg_delivery_minutes,
            window_start,
            window_end
        FROM HOP('fulfilled', 'order_time', INTERVAL '{window_seconds} seconds')
        GROUP BY region, window_start, window_end
        ORDER BY region
        """,
        schema=RevenueByRegionSchema,
    ).trigger(window=trigger)

    return app


class ObservableSink:
    """Sink that prints results and optionally forwards to Kafka."""

    def __init__(self, output_mode: str, kafka_sink: KafkaSink | None, verbose: bool):
        self._output_mode = output_mode
        self._kafka_sink = kafka_sink
        self._verbose = verbose
        self._windows_fired = 0

    async def start(self) -> None:
        if self._kafka_sink:
            await self._kafka_sink.start()

    async def write(self, batch: pa.RecordBatch) -> None:
        self._windows_fired += 1

        if self._output_mode in ("console", "both"):
            self._print_results(batch)

        if self._output_mode in ("kafka", "both") and self._kafka_sink:
            await self._kafka_sink.write(batch)

    async def stop(self) -> None:
        if self._kafka_sink:
            await self._kafka_sink.stop()

    def _print_results(self, batch: pa.RecordBatch) -> None:
        rows = batch.to_pylist()
        if not rows:
            return

        window_end = rows[0]["window_end"]
        window_start = rows[0]["window_start"]

        print(f"\n{'=' * 60}")
        print(f"Window: {window_start.strftime('%H:%M')} - {window_end.strftime('%H:%M')}")
        print(f"{'=' * 60}")
        print(f"{'Region':<12} {'Revenue':>12} {'Orders':>8} {'Avg Delivery':>14}")
        print(f"{'-' * 12} {'-' * 12} {'-' * 8} {'-' * 14}")

        total_revenue = 0.0
        total_orders = 0
        for row in rows:
            avg_del = row["avg_delivery_minutes"]
            avg_del_str = f"{avg_del:.1f}min" if avg_del is not None else "N/A"
            print(
                f"{row['region']:<12} ${row['total_revenue']:>11,.2f} "
                f"{row['num_orders']:>8,} {avg_del_str:>14}"
            )
            total_revenue += row["total_revenue"]
            total_orders += row["num_orders"]

        print(f"{'-' * 12} {'-' * 12} {'-' * 8} {'-' * 14}")
        print(f"{'TOTAL':<12} ${total_revenue:>11,.2f} {total_orders:>8,}")


class ProgressReporter:
    """Reports pipeline progress periodically."""

    def __init__(self, sources: dict[str, KafkaSource], interval: float = 5.0):
        self._sources = sources
        self._interval = interval
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._report_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _report_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._interval)
            self._print_progress()

    def _print_progress(self) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        parts = []

        for name, source in self._sources.items():
            wm = source.watermark
            if wm:
                if wm.tzinfo is None:
                    wm = wm.replace(tzinfo=dt.timezone.utc)
                lag = (now - wm).total_seconds()
                parts.append(f"{name}: {wm.strftime('%H:%M:%S')} (lag: {lag:.0f}s)")
            else:
                parts.append(f"{name}: (no watermark)")

        print(f"[{now.strftime('%H:%M:%S')}] {' | '.join(parts)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the streaming pipeline")

    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--schema-registry", default="http://localhost:8081", help="Schema Registry URL")
    parser.add_argument(
        "--start",
        type=lambda s: dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc),
        default=None,
        help="Start processing from this time (default: earliest)",
    )
    parser.add_argument("--window", default="15m", help="Window size, e.g., '15m', '1h'")
    parser.add_argument("--trigger", default="1m", help="Trigger interval, e.g., '1m', '5m'")
    parser.add_argument("--orders-topic", default="orders", help="Orders topic")
    parser.add_argument("--deliveries-topic", default="deliveries", help="Deliveries topic")
    parser.add_argument("--output-topic", default="revenue-by-region", help="Output topic")
    parser.add_argument(
        "--output-mode", choices=["kafka", "console", "both"], default="both", help="Where to write results"
    )
    parser.add_argument("--serialization", choices=["json", "avro"], default="json", help="Serialization format")
    parser.add_argument("--verbose", action="store_true", help="Print detailed progress")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging for quackflow")
    parser.add_argument("--group-suffix", default="", help="Suffix to add to consumer group IDs")

    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    window = parse_duration(args.window)
    trigger = parse_duration(args.trigger)

    app = build_pipeline(window, trigger)

    if args.serialization == "avro":
        from confluent_kafka.schema_registry import SchemaRegistryClient

        from quackflow.connectors.kafka import ConfluentAvroDeserializer

        sr = SchemaRegistryClient({"url": args.schema_registry})
        order_deser = ConfluentAvroDeserializer(sr)
        delivery_deser = ConfluentAvroDeserializer(sr)
    else:
        order_deser = TimestampAwareJsonDeserializer(["order_time"])
        delivery_deser = TimestampAwareJsonDeserializer(["delivery_time"])

    order_source = KafkaSource(
        topic=args.orders_topic,
        time_notion=EventTimeNotion("order_time"),
        bootstrap_servers=args.bootstrap_servers,
        group_id=f"quackflow-demo-orders{args.group_suffix}",
        schema=OrderSchema,
        value_deserializer=order_deser,
    )

    delivery_source = KafkaSource(
        topic=args.deliveries_topic,
        time_notion=EventTimeNotion("delivery_time"),
        bootstrap_servers=args.bootstrap_servers,
        group_id=f"quackflow-demo-deliveries{args.group_suffix}",
        schema=DeliverySchema,
        value_deserializer=delivery_deser,
    )

    kafka_sink = None
    if args.output_mode in ("kafka", "both"):
        kafka_sink = KafkaSink(
            topic=args.output_topic,
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=TimestampAwareJsonSerializer(),
        )

    observable_sink = ObservableSink(args.output_mode, kafka_sink, args.verbose)

    sources = {"orders": order_source, "deliveries": delivery_source}
    reporter = ProgressReporter(sources) if args.verbose else None

    runtime = Runtime(
        app,
        sources={"orders": order_source, "deliveries": delivery_source},
        sinks={"revenue_by_region": observable_sink},
    )

    print(f"Starting pipeline (window={args.window}, trigger={args.trigger})...")
    print("Press Ctrl+C to stop\n")

    try:
        if reporter:
            await reporter.start()

        start_time = args.start or dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)
        await runtime.execute(start=start_time)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        if reporter:
            await reporter.stop()


if __name__ == "__main__":
    asyncio.run(main())
