import datetime as dt

import pyarrow as pa

from quackflow.time_notion import EventTimeNotion, ProcessingTimeNotion, TimeNotion


class TestTimeNotionProtocol:
    def test_event_time_notion_implements_protocol(self):
        notion = EventTimeNotion(column="timestamp")

        assert isinstance(notion, TimeNotion)

    def test_processing_time_notion_implements_protocol(self):
        notion = ProcessingTimeNotion()

        assert isinstance(notion, TimeNotion)


class TestEventTimeNotion:
    def test_compute_watermark_returns_max_timestamp(self):
        notion = EventTimeNotion(column="timestamp")
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "timestamp": [
                    dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2024, 1, 1, 11, 0, tzinfo=dt.timezone.utc),
                ],
            }
        )

        watermark = notion.compute_watermark(batch)

        assert watermark == dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)

    def test_compute_watermark_with_allowed_lateness(self):
        notion = EventTimeNotion(column="timestamp", allowed_lateness=dt.timedelta(minutes=5))
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1],
                "timestamp": [dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)],
            }
        )

        watermark = notion.compute_watermark(batch)

        assert watermark == dt.datetime(2024, 1, 1, 11, 55, tzinfo=dt.timezone.utc)


class TestProcessingTimeNotion:
    def test_compute_watermark_returns_current_time(self):
        notion = ProcessingTimeNotion()
        batch = pa.RecordBatch.from_pydict({"id": [1]})

        before = dt.datetime.now(dt.timezone.utc)
        watermark = notion.compute_watermark(batch)
        after = dt.datetime.now(dt.timezone.utc)

        assert before <= watermark <= after

    def test_compute_watermark_with_allowed_lateness(self):
        notion = ProcessingTimeNotion(allowed_lateness=dt.timedelta(seconds=30))
        batch = pa.RecordBatch.from_pydict({"id": [1]})

        before = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=30)
        watermark = notion.compute_watermark(batch)
        after = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=30)

        assert before <= watermark <= after
