"""
Bronze consumer service — Step 3 of the pipeline.

Consumes from Kafka and materialises records as the bronze layer.

Exceptions:
  - InvalidRecordError : malformed Kafka message (logged, message skipped)
  - ConsumerError      : Kafka connection lost after all retries (fatal)
  - IcebergWriteError  : bronze write failed after all retries (fatal)
  - MaxRetriesExceededError : any operation exhausted its retry budget (fatal)
"""

import logging
import os

from economics_pipeline.config import get_settings
from economics_pipeline.dao.iceberg_dao_read_write import get_bronze_writer
from economics_pipeline.exceptions.base import MaxRetriesExceededError
from economics_pipeline.exceptions.kafka import ConsumerError
from economics_pipeline.exceptions.storage import IcebergWriteError
from economics_pipeline.exceptions.validation import InvalidRecordError
from economics_pipeline.kafka.consumer import BronzeConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

FLUSH_EVERY = 500


def run() -> None:
    settings = get_settings()
    source_file = os.path.join(settings.landing_path, settings.source_filename)
    writer = get_bronze_writer(settings)

    logger.info(
        "Bronze consumer starting | topic=%s group=%s",
        settings.kafka_topic, settings.kafka_group_id,
    )

    count = 0
    skipped = 0

    try:
        with BronzeConsumer(settings, source_file=source_file) as consumer:
            consumer_gen = consumer.consume()
            while True:
                try:
                    record = next(consumer_gen)
                except StopIteration:
                    break
                except InvalidRecordError as exc:
                    logger.warning("Skipping invalid Kafka message: %s", exc)
                    skipped += 1
                    continue
                writer.append(record)
                count += 1
                if count % FLUSH_EVERY == 0:
                    writer.flush()
                    logger.info("Flushed %d bronze records", count)
    except (ConsumerError, MaxRetriesExceededError):
        logger.exception("Fatal Kafka error — flushing buffer before exit")
        writer.flush()
        raise
    except IcebergWriteError:
        logger.exception("Fatal storage error — bronze write failed")
        raise

    writer.flush()
    logger.info(
        "Bronze consumer done — %d written, %d skipped to %s",
        count, skipped, settings.bronze_path,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.critical("Consumer failed: %s", exc, exc_info=True)
        raise SystemExit(1) from exc
