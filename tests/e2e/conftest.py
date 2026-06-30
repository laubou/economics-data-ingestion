"""
E2E test fixtures.

E2E tests run the full 4-step pipeline against a real local Kafka broker
and the local filesystem. Docker must be running — the fixture starts and
stops the stack automatically.

Run: pytest tests/e2e/ -m e2e
"""

import csv
import os
import subprocess
import time

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from economics_pipeline.config.base import PipelineSettings

_BOOTSTRAP = "localhost:9092"
_E2E_TOPIC = "e2e-sales-events"


# ------------------------------------------------------------------ #
# Docker Compose lifecycle                                             #
# ------------------------------------------------------------------ #


@pytest.fixture(scope="session", autouse=True)
def docker_stack():
    """Start kafka + zookeeper before the session, tear down after."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    subprocess.run(
        ["docker", "compose", "up", "-d", "zookeeper", "kafka"],
        cwd=project_root,
        check=True,
    )

    _wait_for_kafka_ready(retries=30, delay=3)
    _create_topic(_E2E_TOPIC, partitions=3)

    yield

    subprocess.run(
        ["docker", "compose", "down", "--volumes", "--remove-orphans"],
        cwd=project_root,
        check=False,
    )


def _wait_for_kafka_ready(retries: int, delay: int) -> None:
    """Wait until Kafka accepts real metadata requests, not just TCP connections."""
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
    """Create the test topic if it doesn't already exist."""
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
# Pipeline settings                                                    #
# ------------------------------------------------------------------ #


@pytest.fixture(scope="session")
def e2e_settings(tmp_path_factory) -> PipelineSettings:
    tmp = tmp_path_factory.mktemp("e2e")
    return PipelineSettings(
        environment="e2e",
        kafka_bootstrap_servers=_BOOTSTRAP,
        kafka_topic=_E2E_TOPIC,
        kafka_group_id="e2e-bronze-group",
        data_base_path=str(tmp),
        s3_bucket="",
        max_rows=20,
    )


@pytest.fixture(scope="session")
def e2e_csv(e2e_settings: PipelineSettings) -> str:
    """Create a minimal CSV in the landing zone for e2e tests."""
    landing_dir = e2e_settings.landing_path
    os.makedirs(landing_dir, exist_ok=True)
    csv_path = os.path.join(landing_dir, e2e_settings.source_filename)

    rows = [
        {
            "Region": "Europe", "Country": "France", "Item Type": "Beverages",
            "Sales Channel": "Online", "Order Priority": "H",
            "Order Date": "1/3/2020", "Ship Date": "1/10/2020",
            "Order ID": str(i), "Units Sold": "10",
            "Unit Price": "9.99", "Unit Cost": "5.50",
            "Total Revenue": "99.90", "Total Cost": "55.00", "Total Profit": "44.90",
        }
        for i in range(1, 21)
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return csv_path
