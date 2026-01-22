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

    def __init__(self, schema_registry: "SchemaRegistryClient"):
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        self._deserializer: Any = AvroDeserializer(schema_registry)  # type: ignore[call-arg]

    def __call__(self, data: bytes, topic: str) -> dict[str, Any]:
        from confluent_kafka.serialization import SerializationContext, MessageField

        result: dict[str, Any] = self._deserializer(data, SerializationContext(topic, MessageField.VALUE))
        return result
