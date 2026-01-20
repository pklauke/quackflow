def _check_kafka_deps() -> None:
    try:
        import confluent_kafka  # noqa: F401
    except ImportError as e:
        raise ImportError("KafkaSource requires 'confluent-kafka'. Install with: pip install quackflow[kafka]") from e


from quackflow.connectors.kafka.source import KafkaSource  # noqa: E402

__all__ = ["KafkaSource"]
