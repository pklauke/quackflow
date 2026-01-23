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


class RevenueByRegionSchema(Schema):
    region = String()
    total_revenue = Float()
    num_orders = Int()
    avg_delivery_minutes = Float()
    window_start = Timestamp()
    window_end = Timestamp()


class FulfilledOrderSchema(Schema):
    order_id = String()
    user_id = String()
    amount = Float()
    region = String()
    order_time = Timestamp()
    delivery_time = Timestamp()
    delivery_minutes = Float()
