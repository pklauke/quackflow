import io
import json
import struct
from unittest.mock import Mock

import fastavro
import pytest

AVRO_SCHEMA = {
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "count", "type": "int"},
    ],
}


def make_confluent_avro_bytes(record: dict, schema: dict, schema_id: int) -> bytes:
    """Create Confluent wire format: magic byte + schema ID + Avro payload."""
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, record)
    avro_bytes = buffer.getvalue()
    return struct.pack(">bI", 0, schema_id) + avro_bytes


class TestJsonDeserializer:
    def test_deserializes_json(self):
        from quackflow.connectors.kafka.deserializers import JsonDeserializer

        deserializer = JsonDeserializer()
        data = json.dumps({"id": "1", "name": "test"}).encode()

        result = deserializer(data, "my-topic")

        assert result == {"id": "1", "name": "test"}

    def test_topic_argument_ignored(self):
        from quackflow.connectors.kafka.deserializers import JsonDeserializer

        deserializer = JsonDeserializer()
        data = json.dumps({"id": "1"}).encode()

        result1 = deserializer(data, "topic-a")
        result2 = deserializer(data, "topic-b")

        assert result1 == result2


class TestConfluentAvroDeserializer:
    def test_requires_schema_registry_client(self):
        from quackflow.connectors.kafka.deserializers import ConfluentAvroDeserializer

        with pytest.raises(TypeError):
            ConfluentAvroDeserializer()  # type: ignore[call-arg]

    def test_deserializes_avro_value(self):
        from confluent_kafka.schema_registry import Schema

        from quackflow.connectors.kafka.deserializers import ConfluentAvroDeserializer

        mock_sr = Mock()
        mock_sr.get_schema.return_value = Schema(json.dumps(AVRO_SCHEMA), "AVRO")

        deserializer = ConfluentAvroDeserializer(mock_sr)
        data = make_confluent_avro_bytes({"id": "abc", "count": 42}, AVRO_SCHEMA, schema_id=1)

        result = deserializer(data, "my-topic")

        assert result == {"id": "abc", "count": 42}
