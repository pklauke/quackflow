# Quackflow

A Python stream processing framework powered by DuckDB.

## What is Quackflow?

Quackflow lets you build real-time data pipelines using SQL. Define sources, transformations, and outputs declaratively, and Quackflow handles watermarks, windowing, and data expiration automatically.

## How it stands out

- **SQL-native**: Write transformations in SQL, not a custom DSL or verbose builder API
- **DuckDB-powered**: Fast, embedded analytics engine with zero external dependencies
- **Automatic windowing**: `HOP()` function handles tumbling and hopping windows
- **Built-in backpressure**: Watermark-based flow control across the entire DAG
- **Distributed mode**: Scale out with Arrow Flight for inter-worker communication

## Installation

```bash
pip install quackflow           # Core only
pip install quackflow[kafka]    # With Kafka support
```

## Quick start

```python
import datetime as dt
from quackflow import Quackflow, Runtime, Schema, String, Int, Float, Timestamp
from quackflow.connectors.kafka import KafkaSource, KafkaSink

# Define schemas
class Order(Schema):
    order_id = String()
    product_category = String()
    amount = Float()
    order_time = Timestamp()

class CategoryRevenue(Schema):
    product_category = String()
    total_revenue = Float()
    order_count = Int()
    window_end = Timestamp()

# Build the pipeline
app = Quackflow()
app.source("orders", schema=Order, ts_col="order_time")
app.view("completed_orders", """
    SELECT order_id, product_category, amount, order_time
    FROM orders
    WHERE amount > 0
""")
app.output("category_revenue", """
    SELECT
        product_category,
        SUM(amount) as total_revenue,
        COUNT(*) as order_count,
        window_end
    FROM HOP('completed_orders', 'order_time', INTERVAL '1 hour')
    GROUP BY product_category, window_start, window_end
""", schema=CategoryRevenue).trigger(window=dt.timedelta(hours=1))

# Run
from quackflow import EventTimeNotion

source = KafkaSource(
    topic="orders",
    time_notion=EventTimeNotion(column="order_time"),
    bootstrap_servers="localhost:9092",
    group_id="quackflow-orders",
    schema=Order,
)

sink = KafkaSink(
    topic="category-revenue",
    bootstrap_servers="localhost:9092",
)

runtime = Runtime(app, sources={"orders": source}, sinks={"category_revenue": sink})
await runtime.execute(start=dt.datetime.now(dt.timezone.utc))
```

## Key concepts

**Sources** ingest data and track watermarks (event time progress).

**Views** are intermediate transformations - filters, joins, enrichments.

**Outputs** are terminal nodes that write to sinks. They define when to emit results via triggers.

**Triggers** control when outputs fire:
- `trigger(records=100)` - emit every 100 records
- `trigger(window=timedelta(minutes=5))` - emit every 5-minute window
