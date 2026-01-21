import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from confluent_kafka.schema_registry import SchemaRegistryClient


class JsonDeserializer:
    """JSON deserializer."""

    def __call__(self, data: bytes, topic: str) -> dict[str, Any]:
        return json.loads(data.decode("utf-8"))


class ConfluentAvroDeserializer:
    """Avro deserializer using Confluent Schema Registry."""

    def __init__(self, schema_registry: "SchemaRegistryClient", *, is_key: bool = False):
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import MessageField

        self._deserializer: Any = AvroDeserializer(schema_registry)  # type: ignore[call-arg]
        self._field = MessageField.KEY if is_key else MessageField.VALUE

    def __call__(self, data: bytes, topic: str) -> dict[str, Any]:
        from confluent_kafka.serialization import SerializationContext

        result: dict[str, Any] = self._deserializer(data, SerializationContext(topic, self._field))
        return result
