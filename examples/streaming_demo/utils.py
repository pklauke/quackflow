import datetime as dt
import json
import re
from typing import Any


def parse_duration(duration_str: str) -> dt.timedelta:
    """Parse a duration string like '15m', '1h', '30s' into a timedelta."""
    match = re.match(r"^(\d+)([smhd])$", duration_str.lower())
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}. Use e.g., '15m', '1h', '30s'")

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "s":
        return dt.timedelta(seconds=value)
    elif unit == "m":
        return dt.timedelta(minutes=value)
    elif unit == "h":
        return dt.timedelta(hours=value)
    elif unit == "d":
        return dt.timedelta(days=value)
    else:
        raise ValueError(f"Unknown duration unit: {unit}")


class TimestampAwareJsonSerializer:
    """JSON serializer that converts datetime to ISO strings."""

    def __call__(self, data: dict[str, Any], topic: str, *, is_key: bool = False) -> bytes:
        payload = {k: v.isoformat() if isinstance(v, dt.datetime) else v for k, v in data.items()}
        return json.dumps(payload).encode()


class TimestampAwareJsonDeserializer:
    """JSON deserializer that parses ISO strings back to datetime."""

    def __init__(self, timestamp_fields: list[str]):
        self._timestamp_fields = timestamp_fields

    def __call__(self, data: bytes, topic: str, *, is_key: bool = False) -> dict[str, Any]:
        record = json.loads(data.decode())
        for field in self._timestamp_fields:
            if field in record and record[field] is not None:
                record[field] = dt.datetime.fromisoformat(record[field])
        return record
