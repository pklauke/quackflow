import json
from typing import TYPE_CHECKING, Any

from quackflow.schema import Schema

if TYPE_CHECKING:
    from confluent_kafka.schema_registry import SchemaRegistryClient


class JsonSerializer:
    """JSON serializer."""

    def __call__(self, data: dict[str, Any], topic: str, *, is_key: bool = False) -> bytes:
        return json.dumps(data).encode("utf-8")


class ConfluentAvroSerializer:
    """Avro serializer using Confluent Schema Registry."""

    def __init__(
        self,
        schema_registry: "SchemaRegistryClient",
        schema: type[Schema],
    ):
        from confluent_kafka.schema_registry import Schema as ConfluentSchema
        from confluent_kafka.schema_registry.avro import AvroSerializer

        avro_schema_str = schema.to_avro()
        confluent_schema = ConfluentSchema(avro_schema_str, "AVRO")
        self._serializer: Any = AvroSerializer(schema_registry, confluent_schema)  # type: ignore[call-arg]

    def __call__(self, data: dict[str, Any], topic: str, *, is_key: bool = False) -> bytes:
        from confluent_kafka.serialization import SerializationContext, MessageField

        field = MessageField.KEY if is_key else MessageField.VALUE
        result: bytes = self._serializer(data, SerializationContext(topic, field))
        return result
