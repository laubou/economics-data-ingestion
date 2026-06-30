from __future__ import annotations

import json
import logging
from typing import Any

from kafka import KafkaProducer as _KafkaProducer
from kafka.errors import KafkaError

from ..config.base import PipelineSettings
from ..exceptions.kafka import ProducerError
from ..models.sales import SalesRecord
from ..retry.policy import kafka_retry

logger = logging.getLogger(__name__)


class SalesProducer:
    """
    Wraps kafka-python's KafkaProducer with domain semantics.

    Partitioning: keyed by order_id → all events for the same order
    land on the same partition, preserving ordering per order.

    Delivery: acks="all" ensures the leader waits for all in-sync
    replicas before acknowledging — strongest durability guarantee.

    Retry: the @kafka_retry decorator retries transient errors (broker
    restart, leader election) up to 5 times with exponential backoff.
    On final failure, raises ProducerError wrapping the Kafka cause.
    """

    def __init__(self, settings: PipelineSettings) -> None:
        self._topic = settings.kafka_topic
        self._settings = settings
        self._producer = self._connect()

    @kafka_retry
    def _connect(self) -> _KafkaProducer:
        return _KafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            acks="all",
            retries=3,
            linger_ms=5,
        )

    def send(self, record: SalesRecord) -> Any:
        try:
            return self._producer.send(
                self._topic,
                key=str(record.order_id),
                value=record.model_dump(mode="json"),
            )
        except KafkaError as exc:
            raise ProducerError(
                topic=self._topic, order_id=record.order_id, cause=exc
            ) from exc

    def flush(self) -> None:
        self._producer.flush()
        logger.debug("Producer flushed")

    def close(self) -> None:
        self._producer.close()

    def __enter__(self) -> "SalesProducer":
        return self

    def __exit__(self, *_: object) -> None:
        self.flush()
        self.close()
