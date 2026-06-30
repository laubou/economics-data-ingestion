"""
Integration test fixtures.

Integration tests require a live Kafka broker. Docker must be running —
the fixture starts kafka automatically and tears it down after the session.

Each test gets its own Kafka topic to avoid offset pollution between tests.

Run: pytest tests/integration/ -m integration
"""

import os
import subprocess
import time
import uuid

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from economics_pipeline.config.base import PipelineSettings

_BOOTSTRAP = "localhost:9092"


# ------------------------------------------------------------------ #
# Docker Compose lifecycle — once per session                          #
# ------------------------------------------------------------------ #


@pytest.fixture(scope="session", autouse=True)
def docker_stack():
    """Start kafka + zookeeper before the session, tear down after."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    print("\n[integration] Starting Docker stack...")
    subprocess.run(
        ["docker", "compose", "up", "-d", "zookeeper", "kafka"],
        cwd=project_root,
        check=True,
    )

    _wait_for_kafka_ready(retries=30, delay=3)
    print("[integration] Kafka ready.")

    yield

    print("\n[integration] Stopping Docker stack...")
    subprocess.run(
        ["docker", "compose", "down", "--volumes", "--remove-orphans"],
        cwd=project_root,
        check=False,
    )
    print("[integration] Docker stack stopped.")


def _wait_for_kafka_ready(retries: int, delay: int) -> None:
    from kafka import KafkaProducer
    for attempt in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=_BOOTSTRAP, request_timeout_ms=3000)
            p.close()
            return
        except Exception:
            if attempt < retries - 1:
                time.sleep(delay)
    raise RuntimeError(f"Kafka not ready after {retries * delay}s — check 'docker logs kafka'")


def _create_topic(topic: str, partitions: int = 3) -> None:
    admin = KafkaAdminClient(bootstrap_servers=_BOOTSTRAP, request_timeout_ms=10000)
    try:
        admin.create_topics([
            NewTopic(name=topic, num_partitions=partitions, replication_factor=1)
        ])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


# ------------------------------------------------------------------ #
# Per-test settings — unique topic per test, no offset pollution       #
# ------------------------------------------------------------------ #


@pytest.fixture
def integration_settings(tmp_path) -> PipelineSettings:
    """
    Each test gets its own Kafka topic so offset state never bleeds
    between tests. Without this, test 2 would re-read messages from
    test 1 because the consumer group committed offset stays at 0.
    """
    topic = f"integration-{uuid.uuid4().hex[:8]}"
    _create_topic(topic, partitions=3)

    return PipelineSettings(
        environment="test",
        kafka_bootstrap_servers=_BOOTSTRAP,
        kafka_topic=topic,
        kafka_group_id=f"group-{topic}",
        data_base_path=str(tmp_path),
        s3_bucket="",
        max_rows=5,
    )
