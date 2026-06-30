"""
Producer service — Step 2 of the pipeline.

Reads the landed CSV row-by-row and publishes each record to Kafka.
Keyed by order_id for consistent partition assignment.

Exceptions:
  - InvalidRecordError : a CSV row cannot be parsed (logged, row skipped)
  - ProducerError      : Kafka send failed after all retries (fatal, exits)
  - MaxRetriesExceededError : broker unreachable after 5 attempts (fatal)
"""

import csv
import logging
import os

from economics_pipeline.config import get_settings
from economics_pipeline.exceptions.base import MaxRetriesExceededError
from economics_pipeline.exceptions.kafka import ProducerError
from economics_pipeline.exceptions.validation import InvalidRecordError
from economics_pipeline.kafka.producer import SalesProducer
from economics_pipeline.models.sales import SalesRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_LOG_EVERY = 10_000


def run() -> None:
    settings = get_settings()
    landing_file = os.path.join(settings.landing_path, settings.source_filename)
    max_rows = settings.max_rows

    logger.info(
        "Producer starting | file=%s max_rows=%s topic=%s",
        landing_file, max_rows or "unlimited", settings.kafka_topic,
    )

    sent = 0
    skipped = 0

    with SalesProducer(settings) as producer:
        with open(landing_file, encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                if max_rows is not None and i >= max_rows:
                    logger.info("Dev row cap reached (%d rows)", max_rows)
                    break
                try:
                    record = SalesRecord.from_csv_row(row)
                    producer.send(record)
                    sent += 1
                except InvalidRecordError as exc:
                    # Bad row: log and skip — do not halt the whole batch
                    logger.warning("Skipping invalid row %d: %s", i, exc)
                    skipped += 1
                except (ProducerError, MaxRetriesExceededError):
                    # Kafka is unreachable — no point continuing
                    logger.exception("Fatal Kafka error at row %d — aborting", i)
                    raise
                if sent % _LOG_EVERY == 0 and sent > 0:
                    logger.info("Sent %d records…", sent)

    logger.info(
        "Producer done — %d sent, %d skipped (invalid) to topic '%s'",
        sent, skipped, settings.kafka_topic,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.critical("Producer failed: %s", exc, exc_info=True)
        raise SystemExit(1) from exc
