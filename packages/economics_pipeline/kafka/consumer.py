from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterator

from kafka import KafkaConsumer as _KafkaConsumer
from kafka.errors import KafkaError

from ..config.base import PipelineSettings
from ..exceptions.kafka import ConsumerError
from ..exceptions.validation import InvalidRecordError
from ..models.sales import BronzeRecord, SalesRecord
from ..retry.policy import kafka_retry

logger = logging.getLogger(__name__)


class BronzeConsumer:
    """
    Consumes from the sales-events topic and emits BronzeRecords.

    Offset management: auto_commit is DISABLED. Offsets must be committed
    explicitly by the caller via commit() — always after a successful
    flush() so that the committed position is never ahead of what is
    durably written to bronze (true at-least-once at the storage level).

    Invalid messages: if a message cannot be parsed into a SalesRecord,
    an InvalidRecordError is raised. The service layer decides whether
    to skip (dead-letter queue) or halt.

    Connection retry: initial broker connection is retried up to 5 times
    with exponential backoff via @kafka_retry.
    """

    def __init__(self, settings: PipelineSettings, source_file: str = "") -> None:
        self._settings = settings
        self._source_file = source_file
        self._consumer = self._connect()

    @kafka_retry
    def _connect(self) -> _KafkaConsumer:
        timeout = self._settings.kafka_consumer_timeout_ms
        kwargs = dict(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=self._settings.kafka_group_id,
        )
        if timeout > 0:
            kwargs["consumer_timeout_ms"] = timeout
        return _KafkaConsumer(self._settings.kafka_topic, **kwargs)

    def consume(self) -> Iterator[BronzeRecord]:
        for message in self._consumer:
            try:
                sales = SalesRecord.model_validate(message.value)
            except Exception as exc:
                raise InvalidRecordError(
                    field="kafka_message",
                    value=message.value,
                    reason=str(exc),
                ) from exc

            try:
                record = BronzeRecord(
                    data=sales,
                    kafka_topic=message.topic,
                    kafka_partition=message.partition,
                    kafka_offset=message.offset,
                    kafka_timestamp_ms=message.timestamp,
                    ingested_at=datetime.now(timezone.utc),
                    source_file=self._source_file,
                )
            except Exception as exc:
                raise ConsumerError(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    cause=exc,
                ) from exc

            yield record

    def commit(self) -> None:
        """Commit all consumed offsets. Call after a successful flush()."""
        self._consumer.commit()

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> "BronzeConsumer":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
