"""
Integration tests: Kafka producer → consumer round-trip.

Requirements: docker-compose up (Kafka must be reachable on localhost:9092).
Run: pytest tests/integration/ -m integration
"""

import threading
import time
from datetime import date

import pytest

from economics_pipeline.config.base import PipelineSettings
from economics_pipeline.kafka.consumer import BronzeConsumer
from economics_pipeline.kafka.producer import SalesProducer
from economics_pipeline.models.sales import SalesRecord


def _make_record(order_id: int) -> SalesRecord:
    return SalesRecord(
        order_id=order_id,
        region="Europe",
        country="France",
        item_type="Beverages",
        sales_channel="Online",
        priority="H",
        order_date=date(2020, 1, 1),
        ship_date=date(2020, 1, 5),
        units_sold=10,
        unit_price=9.99,
        unit_cost=5.50,
        total_revenue=99.90,
        total_cost=55.00,
        total_profit=44.90,
    )


@pytest.mark.integration
class TestProducerConsumerRoundTrip:
    def test_records_reach_consumer(self, integration_settings: PipelineSettings) -> None:
        n = 3
        received: list = []

        # Produce first — consumer replays from earliest offset, no race condition.
        with SalesProducer(integration_settings) as producer:
            for i in range(n):
                producer.send(_make_record(order_id=9000 + i))

        def _consume() -> None:
            with BronzeConsumer(integration_settings) as consumer:
                for record in consumer.consume():
                    received.append(record)
                    if len(received) >= n:
                        break

        thread = threading.Thread(target=_consume, daemon=True)
        thread.start()
        thread.join(timeout=30)

        assert len(received) == n

    def test_bronze_record_has_kafka_metadata(
        self, integration_settings: PipelineSettings
    ) -> None:
        received: list = []

        with SalesProducer(integration_settings) as producer:
            producer.send(_make_record(order_id=9999))

        def _consume() -> None:
            with BronzeConsumer(integration_settings) as consumer:
                for record in consumer.consume():
                    received.append(record)
                    break

        thread = threading.Thread(target=_consume, daemon=True)
        thread.start()
        thread.join(timeout=30)

        assert len(received) == 1
        bronze = received[0]
        assert bronze.kafka_topic == integration_settings.kafka_topic
        assert bronze.kafka_partition >= 0
        assert bronze.kafka_offset >= 0
        assert bronze.ingested_at is not None

    def test_partition_key_is_order_id(
        self, integration_settings: PipelineSettings
    ) -> None:
        """Same order_id must land on the same partition across multiple sends."""
        partitions: list[int] = []

        same_record = _make_record(order_id=42)
        with SalesProducer(integration_settings) as producer:
            producer.send(same_record)
            producer.send(same_record)

        def _consume() -> None:
            with BronzeConsumer(integration_settings) as consumer:
                for record in consumer.consume():
                    partitions.append(record.kafka_partition)
                    if len(partitions) >= 2:
                        break

        thread = threading.Thread(target=_consume, daemon=True)
        thread.start()
        thread.join(timeout=30)

        assert len(partitions) == 2
        assert partitions[0] == partitions[1], "Same key must route to same partition"
