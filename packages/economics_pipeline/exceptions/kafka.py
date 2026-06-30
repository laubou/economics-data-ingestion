from __future__ import annotations
from .base import PipelineError


class KafkaPipelineError(PipelineError):
    """Base for all Kafka-related pipeline errors."""


class ProducerError(KafkaPipelineError):
    """
    A record could not be sent to the Kafka topic after all retry attempts.

    Raised by SalesProducer when the kafka-python future rejects or the
    broker is unreachable after the configured number of retries.
    """

    def __init__(
        self,
        topic: str,
        order_id: int | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.topic = topic
        self.order_id = order_id
        msg = f"Failed to produce to topic '{topic}'"
        if order_id is not None:
            msg += f" (order_id={order_id})"
        super().__init__(msg, cause=cause)


class ConsumerError(KafkaPipelineError):
    """
    A message could not be consumed or deserialized.

    Raised by BronzeConsumer when the Kafka message is malformed
    or the broker connection is lost after all retries.
    """

    def __init__(
        self,
        topic: str,
        partition: int | None = None,
        offset: int | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        location = ""
        if partition is not None and offset is not None:
            location = f" at partition={partition} offset={offset}"
        super().__init__(f"Failed to consume from topic '{topic}'{location}", cause=cause)


class TopicNotFoundError(KafkaPipelineError):
    """
    The expected Kafka topic does not exist.

    Raised on startup when auto-create is disabled and the topic
    was not pre-created by Terraform / kafka-init.
    """

    def __init__(self, topic: str) -> None:
        self.topic = topic
        super().__init__(f"Kafka topic '{topic}' does not exist")
