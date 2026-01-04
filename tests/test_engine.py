import datetime as dt

import pyarrow as pa

from quackflow.engine import Engine
from quackflow.schema import Int, Schema, String, Timestamp


class EventSchema(Schema):
    id = Int()
    user_id = String()
    event_time = Timestamp()


class TestEngineTableCreation:
    def test_create_table_from_schema(self):
        engine = Engine()

        engine.create_table("events", EventSchema)

        tables = engine.query("SELECT table_name FROM information_schema.tables WHERE table_name = 'events'")
        assert tables.num_rows == 1

    def test_create_table_is_empty(self):
        engine = Engine()
        engine.create_table("events", EventSchema)

        result = engine.query("SELECT COUNT(*) as cnt FROM events")

        assert result.column("cnt")[0].as_py() == 0


class TestEngineInsert:
    def test_insert_record_batch(self):
        engine = Engine()
        engine.create_table("events", EventSchema)
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2],
                "user_id": ["alice", "bob"],
                "event_time": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
                ],
            }
        )

        engine.insert("events", batch)

        result = engine.query("SELECT COUNT(*) as cnt FROM events")
        assert result.column("cnt")[0].as_py() == 2

    def test_insert_multiple_batches(self):
        engine = Engine()
        engine.create_table("events", EventSchema)
        batch1 = pa.RecordBatch.from_pydict(
            {
                "id": [1],
                "user_id": ["alice"],
                "event_time": [dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc)],
            }
        )
        batch2 = pa.RecordBatch.from_pydict(
            {
                "id": [2],
                "user_id": ["bob"],
                "event_time": [dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc)],
            }
        )

        engine.insert("events", batch1)
        engine.insert("events", batch2)

        result = engine.query("SELECT COUNT(*) as cnt FROM events")
        assert result.column("cnt")[0].as_py() == 2


class TestEngineQuery:
    def test_query_returns_record_batch(self):
        engine = Engine()
        engine.create_table("events", EventSchema)
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "user_id": ["alice", "bob", "alice"],
                "event_time": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
                ],
            }
        )
        engine.insert("events", batch)

        result = engine.query("SELECT * FROM events WHERE user_id = 'alice'")

        assert isinstance(result, pa.RecordBatch)
        assert result.num_rows == 2

    def test_query_with_aggregation(self):
        engine = Engine()
        engine.create_table("events", EventSchema)
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "user_id": ["alice", "bob", "alice"],
                "event_time": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
                ],
            }
        )
        engine.insert("events", batch)

        result = engine.query("SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id ORDER BY user_id")

        assert result.num_rows == 2
        assert result.column("user_id").to_pylist() == ["alice", "bob"]
        assert result.column("cnt").to_pylist() == [2, 1]
