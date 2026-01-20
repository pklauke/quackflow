from typing import Any


class FakeKafkaMessage:
    def __init__(self, value: bytes, timestamp: tuple[int, int], error: Any = None):
        self._value = value
        self._timestamp = timestamp
        self._error = error

    def value(self) -> bytes:
        return self._value

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp

    def error(self) -> Any:
        return self._error


class FakeKafkaConsumer:
    def __init__(self, messages: list[FakeKafkaMessage] | None = None):
        self._messages = list(messages) if messages else []
        self._index = 0
        self._subscribed_topics: list[str] = []
        self._closed = False
        self._offsets_for_times_called_with: list[Any] | None = None

    def subscribe(self, topics: list[str]) -> None:
        self._subscribed_topics = topics

    def poll(self, timeout: float = 1.0) -> FakeKafkaMessage | None:
        if self._index >= len(self._messages):
            return None
        msg = self._messages[self._index]
        self._index += 1
        return msg

    def close(self) -> None:
        self._closed = True

    def assignment(self) -> list[Any]:
        return []

    def offsets_for_times(self, partitions: list[Any], timeout: float | None = None) -> list[Any]:
        self._offsets_for_times_called_with = partitions
        return partitions

    def seek(self, partition: Any) -> None:
        pass
