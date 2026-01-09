import datetime as dt

import pyarrow as pa
import pytest

from quackflow.app import Quackflow
from quackflow.runtime import Runtime
from quackflow.schema import Float, Int, Schema, String, Timestamp
from quackflow.testing import FakeSink, FakeSource
from quackflow.time_notion import EventTimeNotion


class UserLocationSchema(Schema):
    user_id = String()
    country = String()
    latitude = Float()
    longitude = Float()
    event_time = Timestamp()


class LocationAggSchema(Schema):
    window_start = Timestamp()
    window_end = Timestamp()
    country = String()
    location_bucket = Int()
    total_users = Int()


def make_location_batch(
    user_ids: list[str],
    countries: list[str],
    latitudes: list[float],
    longitudes: list[float],
    times: list[dt.datetime],
) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {
            "user_id": user_ids,
            "country": countries,
            "latitude": latitudes,
            "longitude": longitudes,
            "event_time": times,
        }
    )


class TestGeospatialAggregation:
    @pytest.mark.asyncio
    async def test_location_aggregation_with_window(self):
        time_notion = EventTimeNotion(column="event_time")

        batch = make_location_batch(
            user_ids=["alice", "alice", "bob", "charlie", "david"],
            countries=["US", "US", "US", "UK", "US"],
            latitudes=[36.9, 37.1, 37.2, 51.5, 37.3],
            longitudes=[-121.9, -122.4, -122.4, -0.1, -122.4],
            times=[
                dt.datetime(2024, 1, 1, 10, 0, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 0, 30, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 0, 45, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 1, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2024, 1, 1, 10, 2, 30, tzinfo=dt.timezone.utc),
            ],
        )

        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("user_locations", schema=UserLocationSchema)

        app.view(
            "location_buckets",
            """
            SELECT
                user_id,
                country,
                CAST(FLOOR(latitude) AS INTEGER) AS location_bucket,
                event_time,
                ROW_NUMBER() OVER (PARTITION BY country, user_id ORDER BY event_time DESC) AS row_num
            FROM user_locations
            QUALIFY row_num = 1
            """,
        )

        app.output(
            "results",
            """
            SELECT
                window_start,
                window_end,
                country,
                location_bucket,
                COUNT(user_id) AS total_users
            FROM HOP('location_buckets', 'event_time', INTERVAL '2 minutes')
            GROUP BY window_start, window_end, country, location_bucket
            """,
            schema=LocationAggSchema,
        ).trigger(window=dt.timedelta(minutes=1))

        runtime = Runtime(
            app,
            sources={"user_locations": source},
            sinks={"results": sink},
        )
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 2, tzinfo=dt.timezone.utc),
        )

        def extract(row):
            return {
                "country": row["country"],
                "location_bucket": row["location_bucket"],
                "total_users": row["total_users"],
            }

        results = [[extract(r) for r in batch] for batch in sink.to_dicts()]

        assert len(results) == 2
        assert results[0] == [{"country": "US", "location_bucket": 37, "total_users": 2}]
        assert sorted(results[1], key=lambda r: r["country"]) == [
            {"country": "UK", "location_bucket": 51, "total_users": 1},
            {"country": "US", "location_bucket": 37, "total_users": 2},
        ]


class OrderSchema(Schema):
    order_id = String()
    product_id = String()
    quantity = Int()
    event_time = Timestamp()


class ProductSchema(Schema):
    product_id = String()
    category = String()
    price = Float()
    event_time = Timestamp()


class RevenueAggSchema(Schema):
    window_end = Timestamp()
    category = String()
    total_revenue = Float()
    order_count = Int()


class TestJoinWithMultipleSources:
    @pytest.mark.asyncio
    async def test_join_with_different_window_sizes(self):
        orders_time_notion = EventTimeNotion(column="event_time")
        products_time_notion = EventTimeNotion(column="event_time")

        orders_batch = pa.RecordBatch.from_pydict(
            {
                "order_id": ["o1", "o2", "o3", "o4", "o5"],
                "product_id": ["p1", "p2", "p1", "p3", "p1"],
                "quantity": [2, 1, 3, 1, 1],
                "event_time": [
                    dt.datetime(2024, 1, 1, 10, 0, 10, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 0, 20, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 0, 40, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 1, 10, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 2, 30, tzinfo=dt.timezone.utc),
                ],
            }
        )

        products_batch = pa.RecordBatch.from_pydict(
            {
                "product_id": ["p1", "p2", "p3", "p4"],
                "category": ["electronics", "clothing", "electronics", "furniture"],
                "price": [100.0, 50.0, 200.0, 300.0],
                "event_time": [
                    dt.datetime(2024, 1, 1, 10, 0, 5, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 0, 5, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 0, 5, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 10, 2, 30, tzinfo=dt.timezone.utc),
                ],
            }
        )

        orders_source = FakeSource([orders_batch], orders_time_notion)
        products_source = FakeSource([products_batch], products_time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("orders", schema=OrderSchema)
        app.source("products", schema=ProductSchema)

        app.view(
            "orders_windowed",
            """
            SELECT order_id, product_id, quantity, window_end
            FROM HOP('orders', 'event_time', INTERVAL '1 minute')
            """,
        )

        app.view(
            "products_windowed",
            """
            SELECT product_id, category, price, window_end
            FROM HOP('products', 'event_time', INTERVAL '2 minutes')
            """,
        )

        app.view(
            "orders_with_products",
            """
            SELECT
                o.order_id,
                o.product_id,
                o.quantity,
                p.category,
                p.price,
                o.quantity * p.price AS line_total,
                o.window_end
            FROM orders_windowed o
            JOIN products_windowed p
              ON o.product_id = p.product_id
             AND o.window_end = p.window_end
            """,
        )

        app.output(
            "results",
            """
            SELECT
                window_end,
                category,
                SUM(line_total) AS total_revenue,
                COUNT(*) AS order_count
            FROM orders_with_products
            GROUP BY window_end, category
            """,
            schema=RevenueAggSchema,
        ).trigger(window=dt.timedelta(minutes=1))

        runtime = Runtime(
            app,
            sources={"orders": orders_source, "products": products_source},
            sinks={"results": sink},
        )
        await runtime.execute(
            start=dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 1, 1, 10, 2, tzinfo=dt.timezone.utc),
        )

        def extract(row):
            return {
                "category": row["category"],
                "total_revenue": row["total_revenue"],
                "order_count": row["order_count"],
            }

        all_results = [extract(r) for batch in sink.to_dicts() for r in batch]

        electronics = [r for r in all_results if r["category"] == "electronics"]
        clothing = [r for r in all_results if r["category"] == "clothing"]

        assert sum(r["total_revenue"] for r in electronics) == 700.0
        assert sum(r["order_count"] for r in electronics) == 3
        assert sum(r["total_revenue"] for r in clothing) == 50.0
        assert sum(r["order_count"] for r in clothing) == 1
