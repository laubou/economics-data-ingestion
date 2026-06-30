"""
Iceberg read-only DAO.

Responsibility: stream records out of the lake layer.
Never mutates any table or file — safe to run in parallel with writers.

Two implementations:
  LocalBronzeReader  — reads NDJSON files (dev/tests)
  CloudBronzeReader  — scans the real Iceberg table via PyIceberg (production)

The factory dispatches on settings.is_cloud.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterator

from ..config.base import PipelineSettings
from ..models.sales import BronzeRecord

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════ #
# Local implementation (dev / tests)                                           #
# ═══════════════════════════════════════════════════════════════════════════ #


class LocalBronzeReader:
    """
    Streams bronze records from local NDJSON files, sorted by filename
    (which encodes partition and offset, so order is deterministic).
    """

    def __init__(self, settings: PipelineSettings) -> None:
        self._path = settings.bronze_path

    def read_all(self) -> Iterator[BronzeRecord]:
        if not os.path.isdir(self._path):
            logger.warning("Bronze path does not exist: %s", self._path)
            return
        for filename in sorted(os.listdir(self._path)):
            if not filename.endswith(".json"):
                continue
            filepath = os.path.join(self._path, filename)
            with open(filepath) as f:
                yield BronzeRecord.model_validate(json.load(f))


# ═══════════════════════════════════════════════════════════════════════════ #
# Cloud implementation (production — PyIceberg + Glue + S3)                   #
# ═══════════════════════════════════════════════════════════════════════════ #


class CloudBronzeReader:
    """
    Streams BronzeRecord from the Iceberg bronze table in S3 via PyIceberg.

    Records are returned in insertion order within each Iceberg snapshot.
    The reader performs a full table scan — suitable for the transformer_silver
    service which processes every bronze record exactly once.

    For incremental re-processing (e.g., resume after failure), a snapshot-id
    watermark could be added; omitted here for simplicity.
    """

    def __init__(self, settings: PipelineSettings) -> None:
        from pyiceberg.catalog import load_catalog
        self._settings = settings
        catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "glue.region": settings.aws_region,
            },
        )
        self._table = catalog.load_table(
            (settings.glue_database, "sales_bronze")
        )

    def read_all(self) -> Iterator[BronzeRecord]:
        """Yield every BronzeRecord in the Iceberg bronze table."""
        arrow_table = self._table.scan().to_arrow()
        for row in arrow_table.to_pylist():
            yield self._from_row(row)

    @staticmethod
    def _from_row(row: dict[str, Any]) -> BronzeRecord:
        """Reconstruct a BronzeRecord from a flattened Iceberg row."""
        from ..models.sales import SalesRecord
        data = SalesRecord(
            order_id=row["order_id"],
            region=row["region"],
            country=row["country"],
            item_type=row["item_type"],
            sales_channel=row["sales_channel"],
            priority=row["priority"],
            order_date=row["order_date"],
            ship_date=row["ship_date"],
            units_sold=row["units_sold"],
            unit_price=row["unit_price"],
            unit_cost=row["unit_cost"],
            total_revenue=row["total_revenue"],
            total_cost=row["total_cost"],
            total_profit=row["total_profit"],
        )
        return BronzeRecord(
            data=data,
            kafka_topic=row["kafka_topic"],
            kafka_partition=row["kafka_partition"],
            kafka_offset=row["kafka_offset"],
            kafka_timestamp_ms=row["kafka_timestamp_ms"],
            ingested_at=row["ingested_at"],
            source_file=row["source_file"],
        )


# ═══════════════════════════════════════════════════════════════════════════ #
# Factory                                                                       #
# ═══════════════════════════════════════════════════════════════════════════ #


def get_bronze_reader(
    settings: PipelineSettings,
) -> LocalBronzeReader | CloudBronzeReader:
    """Return the appropriate BronzeReader for the current environment."""
    if settings.is_cloud:
        return CloudBronzeReader(settings)
    return LocalBronzeReader(settings)
