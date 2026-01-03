import datetime as dt
import typing

import pyarrow as pa
import pyarrow.compute as pc


@typing.runtime_checkable
class TimeNotion(typing.Protocol):
    def compute_watermark(self, batch: pa.RecordBatch) -> dt.datetime: ...


class EventTimeNotion:
    def __init__(self, column: str, allowed_lateness: dt.timedelta = dt.timedelta(0)):
        self.column = column
        self.allowed_lateness = allowed_lateness

    def compute_watermark(self, batch: pa.RecordBatch) -> dt.datetime:
        max_ts: dt.datetime = pc.max(batch.column(self.column)).as_py()  # type: ignore[attr-defined]
        return max_ts - self.allowed_lateness


class ProcessingTimeNotion:
    def __init__(self, allowed_lateness: dt.timedelta = dt.timedelta(0)):
        self.allowed_lateness = allowed_lateness

    def compute_watermark(self, batch: pa.RecordBatch) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc) - self.allowed_lateness
