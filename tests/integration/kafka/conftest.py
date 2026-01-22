import contextlib
import os
import uuid

import pytest

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def kafka_available() -> bool:
    """Check if Kafka is available."""
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        # Try to list topics with a short timeout
        admin.list_topics(timeout=5)
        return True
    except Exception:
        return False


def schema_registry_available() -> bool:
    """Check if Schema Registry is available."""
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        sr.get_subjects()
        return True
    except Exception:
        return False


requires_kafka = pytest.mark.skipif(
    not kafka_available(),
    reason=f"Kafka not available at {KAFKA_BOOTSTRAP_SERVERS}",
)

requires_schema_registry = pytest.mark.skipif(
    not schema_registry_available(),
    reason=f"Schema Registry not available at {SCHEMA_REGISTRY_URL}",
)


@pytest.fixture
def unique_topic() -> str:
    """Generate a unique topic name for test isolation."""
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_group_id() -> str:
    """Generate a unique consumer group ID for test isolation."""
    return f"test-group-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def kafka_admin():
    """Create a Kafka AdminClient."""
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


@pytest.fixture
def create_topic(kafka_admin):
    """Factory fixture to create topics."""
    from confluent_kafka.admin import NewTopic

    created_topics: list[str] = []

    def _create(topic: str, num_partitions: int = 1) -> str:
        new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        futures = kafka_admin.create_topics([new_topic])
        for topic_name, future in futures.items():
            try:
                future.result(timeout=10)
                created_topics.append(topic_name)
            except Exception as e:
                if "already exists" not in str(e):
                    raise
        return topic

    yield _create

    # Cleanup: delete created topics
    if created_topics:
        futures = kafka_admin.delete_topics(created_topics)
        for future in futures.values():
            with contextlib.suppress(Exception):
                future.result(timeout=10)


@pytest.fixture
def schema_registry_client():
    """Create a Schema Registry client."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


@pytest.fixture
def bootstrap_servers() -> str:
    """Return the Kafka bootstrap servers."""
    return KAFKA_BOOTSTRAP_SERVERS
