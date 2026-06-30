"""
Unit tests for BronzeConsumer.

Kafka is mocked — no broker needed.
Tests verify that InvalidRecordError and ConsumerError are raised
in the right situations during message consumption.
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from economics_pipeline.exceptions.kafka import ConsumerError
from economics_pipeline.exceptions.validation import InvalidRecordError
from economics_pipeline.kafka.consumer import BronzeConsumer
from economics_pipeline.models.sales import SalesRecord
from tests.factories import make_csv_row


def _make_valid_payload() -> dict:
    """Return a randomized but valid Kafka message payload (model-field names, typed)."""
    from economics_pipeline.models.sales import SalesRecord
    row = make_csv_row()
    record = SalesRecord.from_csv_row(row)
    # Consumer receives a dict of model field names (not CSV column names), typed
    return record.model_dump(mode="python")


def _make_kafka_message(payload: dict, partition: int = 0, offset: int = 0) -> MagicMock:
    msg = MagicMock()
    msg.topic = "sales-events"
    msg.partition = partition
    msg.offset = offset
    msg.timestamp = 1609459200000
    msg.value = payload
    return msg


@pytest.fixture
def mock_kafka_consumer():
    with patch("economics_pipeline.kafka.consumer._KafkaConsumer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


class TestBronzeConsumerExceptions:
    def test_invalid_payload_raises_invalid_record_error(
        self, mock_kafka_consumer: MagicMock, dev_settings
    ) -> None:
        bad_message = _make_kafka_message({"not": "a sales record"})
        mock_kafka_consumer.__iter__ = MagicMock(return_value=iter([bad_message]))

        consumer = BronzeConsumer(dev_settings)
        with pytest.raises(InvalidRecordError) as exc_info:
            list(consumer.consume())

        assert exc_info.value.field == "kafka_message"

    def test_invalid_record_error_is_pipeline_error(
        self, mock_kafka_consumer: MagicMock, dev_settings
    ) -> None:
        from economics_pipeline.exceptions.base import PipelineError
        bad_message = _make_kafka_message({"garbage": True})
        mock_kafka_consumer.__iter__ = MagicMock(return_value=iter([bad_message]))

        consumer = BronzeConsumer(dev_settings)
        with pytest.raises(PipelineError):
            list(consumer.consume())

    def test_valid_message_yields_bronze_record(
        self, mock_kafka_consumer: MagicMock, dev_settings
    ) -> None:
        payload = _make_valid_payload()
        msg = _make_kafka_message(payload, partition=1, offset=42)
        mock_kafka_consumer.__iter__ = MagicMock(return_value=iter([msg]))

        consumer = BronzeConsumer(dev_settings)
        records = list(consumer.consume())

        assert len(records) == 1
        bronze = records[0]
        assert bronze.kafka_partition == 1
        assert bronze.kafka_offset == 42
        # order_id comes from the random payload — check it round-tripped correctly
        assert bronze.data.order_id == payload["order_id"]

    def test_offset_committed_after_successful_yield(
        self, mock_kafka_consumer: MagicMock, dev_settings
    ) -> None:
        msg = _make_kafka_message(_make_valid_payload())
        mock_kafka_consumer.__iter__ = MagicMock(return_value=iter([msg]))

        consumer = BronzeConsumer(dev_settings)
        list(consumer.consume())

        mock_kafka_consumer.commit.assert_called_once()

    def test_offset_not_committed_on_invalid_message(
        self, mock_kafka_consumer: MagicMock, dev_settings
    ) -> None:
        bad_message = _make_kafka_message({"garbage": True})
        mock_kafka_consumer.__iter__ = MagicMock(return_value=iter([bad_message]))

        consumer = BronzeConsumer(dev_settings)
        with pytest.raises(InvalidRecordError):
            list(consumer.consume())

        mock_kafka_consumer.commit.assert_not_called()

    def test_missing_date_field_raises_invalid_record_error(
        self, mock_kafka_consumer: MagicMock, dev_settings
    ) -> None:
        payload = _make_valid_payload()
        payload["order_date"] = "not-a-date"
        msg = _make_kafka_message(payload)
        mock_kafka_consumer.__iter__ = MagicMock(return_value=iter([msg]))

        consumer = BronzeConsumer(dev_settings)
        with pytest.raises(InvalidRecordError):
            list(consumer.consume())

    def test_multiple_valid_messages_all_yielded(
        self, mock_kafka_consumer: MagicMock, dev_settings
    ) -> None:
        messages = [
            _make_kafka_message({**_make_valid_payload(), "order_id": i}, offset=i)
            for i in range(5)
        ]
        mock_kafka_consumer.__iter__ = MagicMock(return_value=iter(messages))

        consumer = BronzeConsumer(dev_settings)
        records = list(consumer.consume())
        assert len(records) == 5
        assert mock_kafka_consumer.commit.call_count == 5
