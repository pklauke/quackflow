import json
from unittest.mock import Mock

from quackflow.schema import Float, Int, Schema, String


class TestJsonSerializer:
    def test_serializes_dict_to_json(self):
        from quackflow.connectors.kafka.serializers import JsonSerializer

        serializer = JsonSerializer()
        data = {"id": "1", "name": "test"}

        result = serializer(data, "my-topic")

        assert json.loads(result.decode("utf-8")) == {"id": "1", "name": "test"}

    def test_topic_argument_ignored(self):
        from quackflow.connectors.kafka.serializers import JsonSerializer

        serializer = JsonSerializer()
        data = {"id": "1"}

        result1 = serializer(data, "topic-a")
        result2 = serializer(data, "topic-b")

        assert result1 == result2


class TestSerializerDeserializerRoundTrip:
    def test_json_round_trip(self):
        from quackflow.connectors.kafka.deserializers import JsonDeserializer
        from quackflow.connectors.kafka.serializers import JsonSerializer

        serializer = JsonSerializer()
        deserializer = JsonDeserializer()
        original = {"id": "123", "name": "test", "value": 42.5}

        serialized = serializer(original, "topic")
        deserialized = deserializer(serialized, "topic")

        assert deserialized == original

    def test_avro_round_trip(self):
        from quackflow.connectors.kafka.deserializers import ConfluentAvroDeserializer
        from quackflow.connectors.kafka.serializers import ConfluentAvroSerializer

        from .conftest import FakeSchemaRegistryClient

        class TestSchema(Schema):
            id = String()
            count = Int()
            score = Float()

        sr = FakeSchemaRegistryClient()
        serializer = ConfluentAvroSerializer(sr, schema=TestSchema)
        deserializer = ConfluentAvroDeserializer(sr)

        original = {"id": "abc", "count": 42, "score": 3.14}

        serialized = serializer(original, "topic")
        deserialized = deserializer(serialized, "topic")

        assert deserialized == original


class TestConfluentAvroSerializerMessageField:
    def test_uses_value_message_field_by_default(self):
        from confluent_kafka.serialization import MessageField

        from quackflow.connectors.kafka.serializers import ConfluentAvroSerializer

        from .conftest import FakeSchemaRegistryClient

        class TestSchema(Schema):
            id = String()

        sr = FakeSchemaRegistryClient()
        serializer = ConfluentAvroSerializer(sr, schema=TestSchema)
        serializer._serializer = Mock(return_value=b"fake")

        serializer({"id": "abc"}, "my-topic")

        ctx = serializer._serializer.call_args[0][1]
        assert ctx.field == MessageField.VALUE

    def test_uses_key_message_field_when_is_key_true(self):
        from confluent_kafka.serialization import MessageField

        from quackflow.connectors.kafka.serializers import ConfluentAvroSerializer

        from .conftest import FakeSchemaRegistryClient

        class TestSchema(Schema):
            id = String()

        sr = FakeSchemaRegistryClient()
        serializer = ConfluentAvroSerializer(sr, schema=TestSchema)
        serializer._serializer = Mock(return_value=b"fake")

        serializer({"id": "abc"}, "my-topic", is_key=True)

        ctx = serializer._serializer.call_args[0][1]
        assert ctx.field == MessageField.KEY
