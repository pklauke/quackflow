"""Debug script to visualize watermark and expiration flow through the DAG.

Pipeline:
    orders ─► filtered_orders ─┐
                               ├─► joined ─► sales_by_category (GROUP BY category)
    products ──────────────────┘

- Join on: product_id
- Group by: category (different partition key)
"""

import asyncio
import datetime as dt
import logging

import pyarrow as pa

# Enable debug logging for quackflow.task
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("quackflow.task").setLevel(logging.DEBUG)

from quackflow.app import Quackflow
from quackflow.runtime import Runtime
from quackflow.schema import Float, Int, Schema, String, Timestamp
from quackflow.testing.fake_sink import FakeSink
from quackflow.testing.fake_source import FakeSource
from quackflow.time_notion import EventTimeNotion


# Schemas
class OrderSchema(Schema):
    order_id = Int()
    user_id = String()
    product_id = Int()
    amount = Float()
    event_time = Timestamp()


class ProductSchema(Schema):
    product_id = Int()
    category = String()
    created_at = Timestamp()


class SalesResultSchema(Schema):
    category = String()
    total_amount = Float()
    order_count = Int()
    window_start = Timestamp()
    window_end = Timestamp()


def make_orders() -> list[pa.RecordBatch]:
    """Create sample order data across multiple batches."""
    base = dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)

    # Batch 1: 10:00-10:02
    batch1 = pa.RecordBatch.from_pydict(
        {
            "order_id": [1, 2, 3],
            "user_id": ["alice", "bob", "charlie"],
            "product_id": [101, 102, 101],  # laptop, phone, laptop
            "amount": [999.0, 599.0, 1299.0],
            "event_time": [
                base + dt.timedelta(minutes=0),
                base + dt.timedelta(minutes=1),
                base + dt.timedelta(minutes=2),
            ],
        },
    )

    # Batch 2: 10:03-10:06
    batch2 = pa.RecordBatch.from_pydict(
        {
            "order_id": [4, 5, 6],
            "user_id": ["david", "eve", "frank"],
            "product_id": [103, 102, 101],  # headphones, phone, laptop
            "amount": [5.0, 649.0, 1199.0],  # 5.0 will be filtered out (< 10)
            "event_time": [
                base + dt.timedelta(minutes=3),
                base + dt.timedelta(minutes=5),
                base + dt.timedelta(minutes=6),
            ],
        },
    )

    # Batch 3: 10:08-10:12
    batch3 = pa.RecordBatch.from_pydict(
        {
            "order_id": [7, 8, 9],
            "user_id": ["grace", "henry", "ivy"],
            "product_id": [103, 101, 102],  # headphones, laptop, phone
            "amount": [149.0, 1399.0, 549.0],
            "event_time": [
                base + dt.timedelta(minutes=8),
                base + dt.timedelta(minutes=10),
                base + dt.timedelta(minutes=12),
            ],
        },
    )

    return [batch1, batch2, batch3]


def make_products() -> list[pa.RecordBatch]:
    """Create sample product data."""
    base = dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)

    # Products arrive early and stay constant
    batch = pa.RecordBatch.from_pydict(
        {
            "product_id": [101, 102, 103],
            "category": ["electronics", "electronics", "accessories"],
            "created_at": [
                base + dt.timedelta(minutes=0),
                base + dt.timedelta(minutes=0),
                base + dt.timedelta(minutes=0),
            ],
        },
    )

    batch1 = pa.RecordBatch.from_pydict(
        {
            "product_id": [101, 102, 103],
            "category": ["electronics", "electronics", "accessories"],
            "created_at": [
                base + dt.timedelta(minutes=3),
                base + dt.timedelta(minutes=3),
                base + dt.timedelta(minutes=3),
            ],
        },
    )

    # Second batch with updated timestamps to advance watermark
    batch2 = pa.RecordBatch.from_pydict(
        {
            "product_id": [101, 102, 103],
            "category": ["electronics", "electronics", "accessories"],
            "created_at": [
                base + dt.timedelta(minutes=6),
                base + dt.timedelta(minutes=6),
                base + dt.timedelta(minutes=6),
            ],
        },
    )

    batch3 = pa.RecordBatch.from_pydict(
        {
            "product_id": [101, 102, 103],
            "category": ["electronics", "electronics", "accessories"],
            "created_at": [
                base + dt.timedelta(minutes=12),
                base + dt.timedelta(minutes=12),
                base + dt.timedelta(minutes=12),
            ],
        },
    )

    return [batch, batch1, batch2, batch3]


async def main():
    print("=" * 80)
    print("Quackflow Debug Flow - Watermark & Expiration Tracing")
    print("=" * 80)
    print()
    print("Pipeline:")
    print("  orders -> filtered_orders -+")
    print("                             +-> joined -> sales_by_category")
    print("  products -----------------+")
    print()
    print("  Join key: product_id")
    print("  Group by: category")
    print("  Window: 5 minutes")
    print()
    print("=" * 80)
    print()

    # Create sources with delay to simulate real-world batch arrival
    orders_source = FakeSource(
        make_orders(), EventTimeNotion(column="event_time"), delay_between_batches=0.5
    )
    products_source = FakeSource(
        make_products(), EventTimeNotion(column="created_at"), delay_between_batches=0.5
    )
    sink = FakeSink()

    # Build pipeline
    app = Quackflow()

    # Sources
    app.source("orders", schema=OrderSchema, ts_col="event_time")
    app.source("products", schema=ProductSchema, ts_col="created_at")

    # Filter orders (amount > 10)
    app.view(
        "filtered_orders",
        """
        SELECT order_id, user_id, product_id, amount, event_time
        FROM orders
        WHERE amount > 10
        """,
    )

    # Join orders with products (join on product_id)
    app.view(
        "joined",
        """
        SELECT
            o.order_id,
            o.user_id,
            o.product_id,
            o.amount,
            p.category,
            o.event_time
        FROM HOP('filtered_orders', 'event_time', INTERVAL '5 minutes') o
        JOIN HOP('products', 'created_at', INTERVAL '5 minutes') p
            ON o.product_id = p.product_id
            AND o.window_end = p.window_end
        """,
    )

    # Aggregate by category (different from join key)
    app.output(
        "sales_by_category",
        """
        SELECT
            category,
            SUM(amount) as total_amount,
            COUNT(*) as order_count,
            window_start,
            window_end
        FROM HOP('joined', 'event_time', INTERVAL '5 minutes')
        GROUP BY category, window_start, window_end
        """,
        schema=SalesResultSchema,
    ).trigger(window=dt.timedelta(minutes=2, seconds=30))

    # Run
    runtime = Runtime(
        app,
        sources={"orders": orders_source, "products": products_source},
        sinks={"sales_by_category": sink},
    )

    start = dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)
    end = dt.datetime(2024, 1, 1, 10, 15, tzinfo=dt.timezone.utc)

    print(f"Running from {start.strftime('%H:%M')} to {end.strftime('%H:%M')}")
    print()

    await runtime.execute(start=start, end=end)

    print()
    print("=" * 80)
    print("Results:")
    print("=" * 80)
    for i, batch_rows in enumerate(sink.to_dicts()):
        for row in batch_rows:
            print(
                f"  {row['category']:12} | "
                f"${row['total_amount']:,.2f} | "
                f"{row['order_count']} orders | "
                f"{row['window_start'].strftime('%H:%M')}-{row['window_end'].strftime('%H:%M')}"
            )


if __name__ == "__main__":
    asyncio.run(main())
