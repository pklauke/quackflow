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
from quackflow._internal.distributed.config import ClusterConfig, WorkerInfo, assign_tasks_to_workers
from quackflow._internal.distributed.worker import DistributedWorkerOrchestrator
from quackflow._internal.execution import ExecutionDAG

from schemas import DeliverySchema, OrderSchema, RevenueByRegionSchema
from utils import TimestampAwareJsonDeserializer, TimestampAwareJsonSerializer, parse_duration


def build_pipeline(window: timedelta, trigger: timedelta) -> Quackflow:
    """Build the streaming pipeline."""
    app = Quackflow()

    app.source("orders", schema=OrderSchema, ts_col="order_time")
    app.source("deliveries", schema=DeliverySchema, ts_col="delivery_time")

    window_seconds = int(window.total_seconds())
    app.view(
        "fulfilled",
        f"""
        SELECT
            o.order_id,
            o.user_id,
            o.amount,
            o.region,
            o.order_time,
            d.delivery_time,
            EXTRACT(EPOCH FROM (d.delivery_time - o.order_time)) / 60.0 AS delivery_minutes,
        FROM HOP('orders', 'order_time', INTERVAL '{window_seconds * 2} seconds') o
        JOIN HOP('deliveries', 'delivery_time', INTERVAL '{window_seconds} seconds') d
            ON o.order_id = d.order_id AND o.window_end = d.window_end
        """,
    )

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
        FROM HOP(fulfilled, 'delivery_time', INTERVAL '{window_seconds} seconds')
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
        for row in batch.to_pylist():
            ws = row['window_start']
            we = row['window_end']
            if ws.tzinfo is not None:
                ws = ws.astimezone(dt.timezone.utc)
            if we.tzinfo is not None:
                we = we.astimezone(dt.timezone.utc)
            window = f"{ws.strftime('%H:%M')}-{we.strftime('%H:%M')}"
            avg_del = row["avg_delivery_minutes"]
            avg_del_str = f"{avg_del:.1f}min" if avg_del is not None else "N/A"
            print(
                f"[{window}] {row['region']}: "
                f"${row['total_revenue']:,.2f} revenue, "
                f"{row['num_orders']} orders, "
                f"{avg_del_str} avg delivery"
            )


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
        help="Start processing from this time (default: 24h ago)",
    )
    parser.add_argument(
        "--end",
        type=lambda s: dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc),
        default=None,
        help="Stop processing at this time (default: run continuously)",
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

    # Distributed mode arguments
    parser.add_argument("--distributed", action="store_true", help="Run in distributed mode")
    parser.add_argument("--num-workers", type=int, default=2, help="Total number of workers in the cluster")
    parser.add_argument("--num-partitions", type=int, default=None, help="Number of partitions (default: same as num-workers)")
    parser.add_argument("--worker-id", type=int, default=0, help="This worker's ID (0 to num-workers-1)")
    parser.add_argument("--base-port", type=int, default=50051, help="Base port for Flight servers (worker N uses base-port + N)")

    return parser.parse_args()


async def run_single_worker(
    app: Quackflow,
    sources: dict,
    sinks: dict,
    args: argparse.Namespace,
    start_time: dt.datetime,
    reporter: ProgressReporter | None,
) -> None:
    """Run the pipeline in single-worker mode."""
    runtime = Runtime(app, sources=sources, sinks=sinks)

    print(f"Starting pipeline (window={args.window}, trigger={args.trigger})...")
    print("Press Ctrl+C to stop\n")

    try:
        if reporter:
            await reporter.start()
        await runtime.execute(start=start_time, end=args.end)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        if reporter:
            await reporter.stop()


async def run_distributed(
    app: Quackflow,
    sources: dict,
    sinks: dict,
    args: argparse.Namespace,
    start_time: dt.datetime,
    reporter: ProgressReporter | None,
) -> None:
    """Run the pipeline in distributed mode."""
    num_workers = args.num_workers
    num_partitions = args.num_partitions or num_workers
    worker_id = args.worker_id
    base_port = args.base_port

    if worker_id < 0 or worker_id >= num_workers:
        raise ValueError(f"worker-id must be between 0 and {num_workers - 1}")

    user_dag = app.compile()
    exec_dag = ExecutionDAG.from_user_dag(user_dag, num_partitions)
    task_assignments = assign_tasks_to_workers(exec_dag, num_workers)

    # Build WorkerInfo for all workers
    workers: dict[str, WorkerInfo] = {}
    for i in range(num_workers):
        wid = f"worker_{i}"
        workers[wid] = WorkerInfo(
            worker_id=wid,
            host="localhost",
            port=base_port + i,
            task_ids=task_assignments[wid],
        )

    cluster_config = ClusterConfig(
        workers=workers,
        exec_dag=exec_dag,
        user_dag=user_dag,
    )

    my_worker_id = f"worker_{worker_id}"
    worker_info = workers[my_worker_id]

    orchestrator = DistributedWorkerOrchestrator(
        worker_info=worker_info,
        cluster_config=cluster_config,
        sources=sources,
        sinks=sinks,
    )

    print(f"Starting distributed worker {worker_id}/{num_workers} on port {worker_info.port}")
    print(f"Tasks: {worker_info.task_ids}")
    print(f"Pipeline: window={args.window}, trigger={args.trigger}")
    print("Press Ctrl+C to stop\n")

    try:
        if reporter:
            await reporter.start()
        await orchestrator.run(start=start_time, end=args.end)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        if reporter:
            await reporter.stop()


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
    sinks = {"revenue_by_region": observable_sink}
    reporter = ProgressReporter(sources) if args.verbose else None

    start_time = args.start or dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)

    if args.distributed:
        await run_distributed(app, sources, sinks, args, start_time, reporter)
    else:
        await run_single_worker(app, sources, sinks, args, start_time, reporter)


if __name__ == "__main__":
    asyncio.run(main())
