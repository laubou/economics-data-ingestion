"""
Silver transformer service — Step 4 of the pipeline.

Reads all bronze records, transforms them, merges into silver with dedup.

Exceptions:
  - IcebergWriteError      : silver write failed after all retries (fatal)
  - MaxRetriesExceededError: storage exhausted its retry budget (fatal)
  - DuplicateRecordError   : only raised in strict mode (default: silent skip)
"""

import logging

from economics_pipeline.config import get_settings
from economics_pipeline.dao.iceberg_dao_read_only import get_bronze_reader
from economics_pipeline.dao.iceberg_dao_read_write import get_silver_writer
from economics_pipeline.exceptions.base import MaxRetriesExceededError
from economics_pipeline.exceptions.storage import IcebergWriteError
from economics_pipeline.transforms.silver import transform_to_silver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

FLUSH_EVERY = 500


def run() -> None:
    settings = get_settings()
    reader = get_bronze_reader(settings)
    writer = get_silver_writer(settings)

    logger.info(
        "Silver transformer starting | bronze=%s silver=%s",
        settings.bronze_path, settings.silver_path,
    )

    written = 0
    skipped = 0

    try:
        for bronze in reader.read_all():
            silver = transform_to_silver(bronze)
            if writer.merge(silver):
                written += 1
            else:
                skipped += 1

            total = written + skipped
            if total % FLUSH_EVERY == 0:
                writer.flush()
                logger.info("Progress: %d written, %d duplicates skipped", written, skipped)

    except (IcebergWriteError, MaxRetriesExceededError):
        logger.exception("Fatal storage error — flushing buffer before exit")
        writer.flush()
        raise

    writer.flush()
    logger.info(
        "Transformation done — %d records written, %d duplicates skipped",
        written, skipped,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.critical("Transformer failed: %s", exc, exc_info=True)
        raise SystemExit(1) from exc
