from dataclasses import dataclass
from typing import Any

from confluent_kafka.schema_registry import Schema


@dataclass
class FakeRegisteredSchema:
    """Fake registered schema returned by FakeSchemaRegistryClient."""

    schema_id: int
    schema: Schema
    subject: str
    version: int
    guid: str | None = None


class FakeSchemaRegistryClient:
    """In-memory Schema Registry for testing without network calls."""

    def __init__(self) -> None:
        self._schemas: dict[int, Schema] = {}
        self._subjects: dict[str, int] = {}
        self._versions: dict[str, int] = {}
        self._next_id = 1

    def register_schema(self, subject: str, schema: Schema) -> int:
        if subject in self._subjects:
            return self._subjects[subject]
        schema_id = self._next_id
        self._next_id += 1
        self._schemas[schema_id] = schema
        self._subjects[subject] = schema_id
        self._versions[subject] = 1
        return schema_id

    def register_schema_full_response(
        self,
        subject: str,
        schema: Schema,
        normalize_schemas: bool = False,
    ) -> FakeRegisteredSchema:
        schema_id = self.register_schema(subject, schema)
        return FakeRegisteredSchema(
            schema_id=schema_id,
            schema=schema,
            subject=subject,
            version=self._versions[subject],
        )

    def get_schema(
        self,
        schema_id: int,
        subject: str | None = None,
        fmt: str | None = None,
    ) -> Schema:
        return self._schemas[schema_id]

    def get_latest_version(self, subject: str) -> FakeRegisteredSchema | None:
        if subject not in self._subjects:
            return None
        schema_id = self._subjects[subject]
        return FakeRegisteredSchema(
            schema_id=schema_id,
            schema=self._schemas[schema_id],
            subject=subject,
            version=self._versions[subject],
        )


class FakeKafkaMessage:
    def __init__(
        self,
        value: bytes,
        timestamp: tuple[int, int],
        key: bytes | None = None,
        error: Any = None,
    ):
        self._value = value
        self._timestamp = timestamp
        self._key = key
        self._error = error

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp

    def error(self) -> Any:
        return self._error


@dataclass
class FakeTopicPartition:
    topic: str
    partition: int
    offset: int = -1


class FakeKafkaConsumer:
    def __init__(self, messages: list[FakeKafkaMessage] | None = None):
        self._messages = list(messages) if messages else []
        self._index = 0
        self._subscribed_topics: list[str] = []
        self._closed = False
        self._offsets_for_times_called_with: list[Any] | None = None
        self._assignment: list[FakeTopicPartition] = []

    def subscribe(self, topics: list[str]) -> None:
        self._subscribed_topics = topics
        self._assignment = [FakeTopicPartition(topic=t, partition=0) for t in topics]

    def poll(self, timeout: float = 1.0) -> FakeKafkaMessage | None:
        if self._index >= len(self._messages):
            return None
        msg = self._messages[self._index]
        self._index += 1
        return msg

    def consume(self, num_messages: int = 1, timeout: float = 1.0) -> list[FakeKafkaMessage]:
        result = []
        for _ in range(num_messages):
            if self._index >= len(self._messages):
                break
            result.append(self._messages[self._index])
            self._index += 1
        return result

    def close(self) -> None:
        self._closed = True

    def assignment(self) -> list[FakeTopicPartition]:
        return self._assignment

    def offsets_for_times(self, partitions: list[Any], timeout: float | None = None) -> list[Any]:
        self._offsets_for_times_called_with = partitions
        return partitions

    def seek(self, partition: Any) -> None:
        pass


class FakeKafkaProducer:
    def __init__(self) -> None:
        self._messages: list[dict[str, Any]] = []
        self._flushed = False

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
    ) -> None:
        self._messages.append({"topic": topic, "value": value, "key": key})

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self._flushed = True
        return 0
