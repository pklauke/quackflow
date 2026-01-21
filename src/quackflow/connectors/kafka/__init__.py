def _check_kafka_deps() -> None:
    try:
        import confluent_kafka  # noqa: F401
    except ImportError as e:
        raise ImportError("KafkaSource requires 'confluent-kafka'. Install with: pip install quackflow[kafka]") from e


from quackflow.connectors.kafka.deserializers import (  # noqa: E402
    ConfluentAvroDeserializer,
    JsonDeserializer,
)
from quackflow.connectors.kafka.serializers import (  # noqa: E402
    ConfluentAvroSerializer,
    JsonSerializer,
)
from quackflow.connectors.kafka.sink import KafkaSink, Serializer  # noqa: E402
from quackflow.connectors.kafka.source import Deserializer, KafkaSource  # noqa: E402

__all__ = [
    "ConfluentAvroDeserializer",
    "ConfluentAvroSerializer",
    "Deserializer",
    "JsonDeserializer",
    "JsonSerializer",
    "KafkaSink",
    "KafkaSource",
    "Serializer",
]
