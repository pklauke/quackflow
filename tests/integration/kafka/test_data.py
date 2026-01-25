import datetime as dt

from quackflow.schema import Float, Int, Schema, String, Timestamp


class OrderSchema(Schema):
    order_id = String()
    user_id = String()
    product_id = String()
    amount = Float()
    region = String()
    order_time = Timestamp()


class DeliverySchema(Schema):
    delivery_id = String()
    order_id = String()
    courier_id = String()
    delivery_time = Timestamp()


class RevenueSchema(Schema):
    region = String()
    total = Float()
    num_orders = Int()
    window_end = Timestamp()


BASE_DATE = dt.datetime(2024, 1, 15, 10, 0, 0, tzinfo=dt.timezone.utc)


def make_orders() -> list[dict]:
    """Generate 20 order records spanning 2 hours (10:00-12:00)."""
    return [
        {
            "order_id": "o-001",
            "user_id": "u-1",
            "product_id": "p-10",
            "amount": 29.99,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=0),
        },
        {
            "order_id": "o-002",
            "user_id": "u-2",
            "product_id": "p-20",
            "amount": 49.99,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=3),
        },
        {
            "order_id": "o-003",
            "user_id": "u-1",
            "product_id": "p-30",
            "amount": 15.00,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=7),
        },
        {
            "order_id": "o-004",
            "user_id": "u-3",
            "product_id": "p-10",
            "amount": 29.99,
            "region": "eu-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=12),
        },
        {
            "order_id": "o-005",
            "user_id": "u-4",
            "product_id": "p-40",
            "amount": 99.99,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=18),
        },
        {
            "order_id": "o-006",
            "user_id": "u-2",
            "product_id": "p-10",
            "amount": 29.99,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=22),
        },
        {
            "order_id": "o-007",
            "user_id": "u-5",
            "product_id": "p-20",
            "amount": 49.99,
            "region": "eu-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=28),
        },
        {
            "order_id": "o-008",
            "user_id": "u-1",
            "product_id": "p-50",
            "amount": 75.00,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=35),
        },
        {
            "order_id": "o-009",
            "user_id": "u-6",
            "product_id": "p-30",
            "amount": 15.00,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=42),
        },
        {
            "order_id": "o-010",
            "user_id": "u-3",
            "product_id": "p-40",
            "amount": 99.99,
            "region": "eu-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=48),
        },
        {
            "order_id": "o-011",
            "user_id": "u-4",
            "product_id": "p-10",
            "amount": 29.99,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=55),
        },
        {
            "order_id": "o-012",
            "user_id": "u-7",
            "product_id": "p-20",
            "amount": 49.99,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=62),
        },
        {
            "order_id": "o-013",
            "user_id": "u-2",
            "product_id": "p-30",
            "amount": 15.00,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=68),
        },
        {
            "order_id": "o-014",
            "user_id": "u-8",
            "product_id": "p-50",
            "amount": 75.00,
            "region": "eu-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=75),
        },
        {
            "order_id": "o-015",
            "user_id": "u-1",
            "product_id": "p-40",
            "amount": 99.99,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=82),
        },
        {
            "order_id": "o-016",
            "user_id": "u-5",
            "product_id": "p-10",
            "amount": 29.99,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=88),
        },
        {
            "order_id": "o-017",
            "user_id": "u-3",
            "product_id": "p-20",
            "amount": 49.99,
            "region": "eu-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=95),
        },
        {
            "order_id": "o-018",
            "user_id": "u-6",
            "product_id": "p-30",
            "amount": 15.00,
            "region": "us-east",
            "order_time": BASE_DATE + dt.timedelta(minutes=100),
        },
        # Late event: arrives after 10:15 watermark but has timestamp 09:55
        {
            "order_id": "o-019",
            "user_id": "u-9",
            "product_id": "p-50",
            "amount": 75.00,
            "region": "us-west",
            "order_time": BASE_DATE - dt.timedelta(minutes=5),
        },
        {
            "order_id": "o-020",
            "user_id": "u-2",
            "product_id": "p-20",
            "amount": 49.99,
            "region": "us-west",
            "order_time": BASE_DATE + dt.timedelta(minutes=105),
        },
    ]


def make_deliveries() -> list[dict]:
    """Generate 15 delivery records lagging orders by 10-45 minutes.

    Orders o-019 and o-020 have no matching delivery (for outer join testing).
    """
    return [
        {
            "delivery_id": "d-001",
            "order_id": "o-001",
            "courier_id": "c-1",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=25),
        },
        {
            "delivery_id": "d-002",
            "order_id": "o-002",
            "courier_id": "c-2",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=40),
        },
        {
            "delivery_id": "d-003",
            "order_id": "o-003",
            "courier_id": "c-1",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=35),
        },
        {
            "delivery_id": "d-004",
            "order_id": "o-004",
            "courier_id": "c-3",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=45),
        },
        {
            "delivery_id": "d-005",
            "order_id": "o-005",
            "courier_id": "c-2",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=50),
        },
        {
            "delivery_id": "d-006",
            "order_id": "o-006",
            "courier_id": "c-1",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=55),
        },
        {
            "delivery_id": "d-007",
            "order_id": "o-007",
            "courier_id": "c-3",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=62),
        },
        {
            "delivery_id": "d-008",
            "order_id": "o-008",
            "courier_id": "c-2",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=70),
        },
        {
            "delivery_id": "d-009",
            "order_id": "o-009",
            "courier_id": "c-1",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=78),
        },
        {
            "delivery_id": "d-010",
            "order_id": "o-010",
            "courier_id": "c-3",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=85),
        },
        {
            "delivery_id": "d-011",
            "order_id": "o-011",
            "courier_id": "c-2",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=92),
        },
        {
            "delivery_id": "d-012",
            "order_id": "o-012",
            "courier_id": "c-1",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=98),
        },
        {
            "delivery_id": "d-013",
            "order_id": "o-013",
            "courier_id": "c-3",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=105),
        },
        {
            "delivery_id": "d-014",
            "order_id": "o-014",
            "courier_id": "c-2",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=112),
        },
        {
            "delivery_id": "d-015",
            "order_id": "o-015",
            "courier_id": "c-1",
            "delivery_time": BASE_DATE + dt.timedelta(minutes=120),
        },
    ]


def orders_in_window(start: dt.datetime, end: dt.datetime) -> list[dict]:
    """Return orders within [start, end) time range."""
    return [o for o in make_orders() if start <= o["order_time"] < end]


def deliveries_in_window(start: dt.datetime, end: dt.datetime) -> list[dict]:
    """Return deliveries within [start, end) time range."""
    return [d for d in make_deliveries() if start <= d["delivery_time"] < end]
