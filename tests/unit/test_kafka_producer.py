"""
Unit tests for SalesProducer.

Kafka is mocked — no broker needed.
Tests verify that the correct pipeline exceptions are raised when
kafka-python raises its own errors.
"""

from unittest.mock import MagicMock, patch

import pytest
from kafka.errors import KafkaError

from economics_pipeline.exceptions.kafka import ProducerError
from economics_pipeline.kafka.producer import SalesProducer
from economics_pipeline.models.sales import SalesRecord
from tests.factories import make_sales_record


def make_record(order_id: int | None = None) -> SalesRecord:
    """Random SalesRecord. Pin order_id when the test checks it in error messages."""
    if order_id is not None:
        return make_sales_record(**{"Order ID": str(order_id)})
    return make_sales_record()


@pytest.fixture
def mock_kafka_producer():
    with patch("economics_pipeline.kafka.producer._KafkaProducer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


class TestSalesProducerExceptions:
    def test_send_raises_producer_error_on_kafka_error(
        self, mock_kafka_producer: MagicMock, dev_settings
    ) -> None:
        mock_kafka_producer.send.side_effect = KafkaError("broker unavailable")

        producer = SalesProducer(dev_settings)
        with pytest.raises(ProducerError) as exc_info:
            producer.send(make_record(order_id=42))

        assert exc_info.value.topic == dev_settings.kafka_topic
        assert exc_info.value.order_id == 42
        assert isinstance(exc_info.value.cause, KafkaError)

    def test_producer_error_message_contains_topic_and_order_id(
        self, mock_kafka_producer: MagicMock, dev_settings
    ) -> None:
        mock_kafka_producer.send.side_effect = KafkaError("timeout")

        producer = SalesProducer(dev_settings)
        with pytest.raises(ProducerError) as exc_info:
            producer.send(make_record(order_id=99))

        assert dev_settings.kafka_topic in str(exc_info.value)
        assert "99" in str(exc_info.value)

    def test_producer_error_is_pipeline_error(
        self, mock_kafka_producer: MagicMock, dev_settings
    ) -> None:
        from economics_pipeline.exceptions.base import PipelineError
        mock_kafka_producer.send.side_effect = KafkaError("err")

        producer = SalesProducer(dev_settings)
        with pytest.raises(PipelineError):
            producer.send(make_record())

    def test_send_succeeds_when_no_error(
        self, mock_kafka_producer: MagicMock, dev_settings
    ) -> None:
        mock_future = MagicMock()
        mock_kafka_producer.send.return_value = mock_future

        producer = SalesProducer(dev_settings)
        result = producer.send(make_record())
        assert result is mock_future

    def test_send_keys_by_order_id(
        self, mock_kafka_producer: MagicMock, dev_settings
    ) -> None:
        producer = SalesProducer(dev_settings)
        producer.send(make_record(order_id=123))

        call_kwargs = mock_kafka_producer.send.call_args
        assert call_kwargs.kwargs["key"] == "123"

    def test_flush_delegates_to_underlying_producer(
        self, mock_kafka_producer: MagicMock, dev_settings
    ) -> None:
        producer = SalesProducer(dev_settings)
        producer.flush()
        mock_kafka_producer.flush.assert_called_once()
