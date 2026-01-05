import datetime as dt

import pyarrow as pa
import pytest

from quackflow.app import Quackflow
from quackflow.runtime import Runtime
from quackflow.schema import Float, Int, Schema, String, Timestamp
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


class FakeSource:
    def __init__(self, batches: list[pa.RecordBatch], time_notion: EventTimeNotion):
        self._batches = batches
        self._time_notion = time_notion
        self._index = 0
        self._watermark: dt.datetime | None = None

    @property
    def watermark(self) -> dt.datetime | None:
        return self._watermark

    async def start(self) -> None:
        pass

    async def seek(self, timestamp: dt.datetime) -> None:
        pass

    async def read(self) -> pa.RecordBatch:
        if self._index >= len(self._batches):
            return pa.RecordBatch.from_pydict(
                {"user_id": [], "country": [], "latitude": [], "longitude": [], "event_time": []},
                schema=self._batches[0].schema if self._batches else None,
            )
        batch = self._batches[self._index]
        self._index += 1
        if batch.num_rows > 0:
            self._watermark = self._time_notion.compute_watermark(batch)
        return batch

    async def stop(self) -> None:
        pass


class FakeSink:
    def __init__(self):
        self.batches: list[pa.RecordBatch] = []

    async def write(self, batch: pa.RecordBatch) -> None:
        self.batches.append(batch)

    def to_dicts(self) -> list[list[dict]]:
        return [
            [{col: batch.column(col)[i].as_py() for col in batch.schema.names} for i in range(batch.num_rows)]
            for batch in self.batches
        ]


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
    async def test_location_aggregation_with_hopping_window(self):
        time_notion = EventTimeNotion(column="event_time")

        # Window [10:00, 10:02) should have: alice, bob in US bucket 37, charlie in UK bucket 51
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
                dt.datetime(2024, 1, 1, 10, 2, 30, tzinfo=dt.timezone.utc),  # after window end
            ],
        )

        source = FakeSource([batch], time_notion)
        sink = FakeSink()

        app = Quackflow()
        app.source("user_locations", source, schema=UserLocationSchema)

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
            depends_on=["user_locations"],
        )

        app.output(
            sink,
            """
            SELECT
                window_start,
                window_end,
                country,
                location_bucket,
                COUNT(user_id) AS total_users
            FROM hopping_window('location_buckets', 'event_time', INTERVAL '2 minutes', INTERVAL '1 minute')
            GROUP BY window_start, window_end, country, location_bucket
            """,
            schema=LocationAggSchema,
            depends_on=["location_buckets"],
        ).trigger(window=dt.timedelta(minutes=1))

        runtime = Runtime(app)
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
