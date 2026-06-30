"""
Iceberg read-write DAO.

Responsibility: append records to bronze, merge records into silver.
Write operations are idempotent: replaying the same offset is a no-op.

Two implementations per table:
  Local*  — NDJSON files, used in dev and tests (no AWS needed)
  Cloud*  — real Parquet + Iceberg via PyIceberg, registered in Glue

The factory functions (get_bronze_writer, get_silver_writer) dispatch on
settings.is_cloud so service code never changes between environments.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from ..config.base import PipelineSettings
from ..exceptions.storage import DuplicateRecordError, IcebergWriteError
from ..models.sales import BronzeRecord, SilverRecord
from ..retry.policy import storage_retry

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════ #
# Local implementations (dev / tests)                                         #
# ═══════════════════════════════════════════════════════════════════════════ #


class LocalBronzeWriter:
    """
    Writes bronze records as NDJSON to a flat local directory.

    Idempotency: filename = p{partition}_o{offset}.json — re-writing
    the same offset produces the same file, so replay is safe.

    In cloud: CloudBronzeWriter is injected instead via the factory.
    """

    def __init__(self, settings: PipelineSettings) -> None:
        self._path = settings.bronze_path
        os.makedirs(self._path, exist_ok=True)
        self._buffer: list[BronzeRecord] = []

    def append(self, record: BronzeRecord) -> None:
        self._buffer.append(record)

    @storage_retry
    def flush(self) -> None:
        if not self._buffer:
            return
        try:
            self._buffer.sort(key=lambda r: (r.kafka_partition, r.kafka_offset))
            first = self._buffer[0]
            filename = f"p{first.kafka_partition}_o{first.kafka_offset}.ndjson"
            filepath = os.path.join(self._path, filename)
            with open(filepath, "w") as f:
                for record in self._buffer:
                    f.write(json.dumps(record.model_dump(mode="json"), default=str) + "\n")
            logger.debug("Bronze batch → %s (%d records)", filepath, len(self._buffer))
        except OSError as exc:
            raise IcebergWriteError("sales_bronze", cause=exc) from exc
        finally:
            count = len(self._buffer)
            self._buffer.clear()
            if count:
                logger.info("Flushed %d bronze records", count)


class LocalSilverWriter:
    """
    Writes silver records as NDJSON, partitioned by year/month.

    Deduplication: source_kafka_offset is the idempotency key.
    On first run the seen-set is empty. On replay, keys pre-loaded
    from existing files prevent double-writes — same semantic as
    Iceberg MERGE INTO ON source_kafka_offset.

    Raises DuplicateRecordError only when `strict=True`; otherwise
    duplicates are silently skipped (default, production-safe behaviour).
    """

    def __init__(self, settings: PipelineSettings, *, strict: bool = False) -> None:
        self._base = settings.silver_path
        self._strict = strict
        self._seen: set[str] = self._load_existing_keys()
        self._buffer: list[SilverRecord] = []

    def _load_existing_keys(self) -> set[str]:
        seen: set[str] = set()
        if not os.path.isdir(self._base):
            return seen
        for root, _, files in os.walk(self._base):
            for filename in files:
                if not filename.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(root, filename)) as f:
                        seen.add(json.load(f)["source_kafka_offset"])
                except (json.JSONDecodeError, KeyError, OSError):
                    pass
        return seen

    def merge(self, record: SilverRecord) -> bool:
        if record.source_kafka_offset in self._seen:
            if self._strict:
                raise DuplicateRecordError(record.source_kafka_offset)
            logger.debug("Duplicate skipped: %s", record.source_kafka_offset)
            return False
        self._seen.add(record.source_kafka_offset)
        self._buffer.append(record)
        return True

    @storage_retry
    def flush(self) -> None:
        try:
            for record in self._buffer:
                partition_dir = os.path.join(
                    self._base,
                    f"year={record.order_year}",
                    f"month={record.order_month:02d}",
                )
                os.makedirs(partition_dir, exist_ok=True)
                filename = f"order_{record.order_id}_{record.source_kafka_offset}.json"
                with open(os.path.join(partition_dir, filename), "w") as f:
                    json.dump(record.model_dump(mode="json"), f, default=str)
                logger.debug("Silver → %s/%s", partition_dir, filename)
        except OSError as exc:
            raise IcebergWriteError("sales_silver", cause=exc) from exc
        finally:
            count = len(self._buffer)
            self._buffer.clear()
            if count:
                logger.info("Flushed %d silver records", count)


# ═══════════════════════════════════════════════════════════════════════════ #
# Cloud implementations (production — PyIceberg + Glue + S3)                  #
# ═══════════════════════════════════════════════════════════════════════════ #


def _build_bronze_schema() -> Any:
    """Return the PyIceberg Schema for the bronze table."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DateType, DoubleType, IntegerType, LongType,
        NestedField, StringType, TimestamptzType,
    )
    return Schema(
        NestedField(1,  "order_id",           LongType(),        required=True),
        NestedField(2,  "region",             StringType(),      required=True),
        NestedField(3,  "country",            StringType(),      required=True),
        NestedField(4,  "item_type",          StringType(),      required=True),
        NestedField(5,  "sales_channel",      StringType(),      required=True),
        NestedField(6,  "priority",           StringType(),      required=True),
        NestedField(7,  "order_date",         DateType(),        required=True),
        NestedField(8,  "ship_date",          DateType(),        required=True),
        NestedField(9,  "units_sold",         IntegerType(),     required=True),
        NestedField(10, "unit_price",         DoubleType(),      required=True),
        NestedField(11, "unit_cost",          DoubleType(),      required=True),
        NestedField(12, "total_revenue",      DoubleType(),      required=True),
        NestedField(13, "total_cost",         DoubleType(),      required=True),
        NestedField(14, "total_profit",       DoubleType(),      required=True),
        NestedField(15, "kafka_topic",        StringType(),      required=True),
        NestedField(16, "kafka_partition",    IntegerType(),     required=True),
        NestedField(17, "kafka_offset",       LongType(),        required=True),
        NestedField(18, "kafka_timestamp_ms", LongType(),        required=True),
        NestedField(19, "ingested_at",        TimestamptzType(), required=True),
        NestedField(20, "source_file",        StringType(),      required=True),
    )


def _build_silver_schema() -> Any:
    """Return the PyIceberg Schema for the silver table."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DateType, DecimalType, IntegerType, LongType,
        NestedField, StringType, TimestamptzType,
    )
    return Schema(
        NestedField(1,  "order_id",              LongType(),           required=True),
        NestedField(2,  "region",                StringType(),         required=True),
        NestedField(3,  "country",               StringType(),         required=True),
        NestedField(4,  "item_type",             StringType(),         required=True),
        NestedField(5,  "sales_channel",         StringType(),         required=True),
        NestedField(6,  "priority",              StringType(),         required=True),
        NestedField(7,  "order_date",            DateType(),           required=True),
        NestedField(8,  "ship_date",             DateType(),           required=True),
        NestedField(9,  "order_year",            IntegerType(),        required=True),
        NestedField(10, "order_month",           IntegerType(),        required=True),
        NestedField(11, "lead_time_days",        IntegerType(),        required=True),
        NestedField(12, "units_sold",            IntegerType(),        required=True),
        NestedField(13, "unit_price",            DecimalType(18, 4),   required=True),
        NestedField(14, "unit_cost",             DecimalType(18, 4),   required=True),
        NestedField(15, "total_revenue",         DecimalType(18, 4),   required=True),
        NestedField(16, "total_cost",            DecimalType(18, 4),   required=True),
        NestedField(17, "total_profit",          DecimalType(18, 4),   required=True),
        NestedField(18, "margin_pct",            DecimalType(6, 2),    required=True),
        NestedField(19, "bronze_ingested_at",    TimestamptzType(),    required=True),
        NestedField(20, "silver_transformed_at", TimestamptzType(),    required=True),
        NestedField(21, "source_kafka_offset",   StringType(),         required=True),
    )


def _build_silver_partition_spec(schema: Any) -> Any:
    """Partition silver by order_year then order_month (identity transforms)."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform
    return PartitionSpec(
        PartitionField(
            source_id=schema.find_field("order_year").field_id,
            field_id=1000,
            transform=IdentityTransform(),
            name="order_year",
        ),
        PartitionField(
            source_id=schema.find_field("order_month").field_id,
            field_id=1001,
            transform=IdentityTransform(),
            name="order_month",
        ),
    )


def _glue_catalog(settings: PipelineSettings) -> Any:
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        "glue",
        **{
            "type": "glue",
            "glue.region": settings.aws_region,
        },
    )


class CloudBronzeWriter:
    """
    Appends BronzeRecord to the real Iceberg bronze table in S3 + Glue.

    The table is created automatically on first run if it does not exist.
    Each `flush()` call results in one Iceberg snapshot — atomic and
    consistent under concurrent writers.
    """

    def __init__(self, settings: PipelineSettings) -> None:
        import pyarrow as pa  # noqa: F401 — verified available at runtime
        self._settings = settings
        self._catalog = _glue_catalog(settings)
        self._table = self._get_or_create_table()
        self._buffer: list[BronzeRecord] = []

    def _get_or_create_table(self) -> Any:
        from pyiceberg.exceptions import NoSuchTableError
        table_id = (self._settings.glue_database, "sales_bronze")
        try:
            return self._catalog.load_table(table_id)
        except NoSuchTableError:
            logger.info("Creating Iceberg table %s", table_id)
            return self._catalog.create_table(
                table_id,
                schema=_build_bronze_schema(),
                location=self._settings.bronze_path,
                properties={
                    "format-version": "2",
                    "write.parquet.compression-codec": "zstd",
                },
            )

    @staticmethod
    def _to_row(record: BronzeRecord) -> dict[str, Any]:
        """Flatten BronzeRecord.data into a single dict for PyArrow."""
        d = record.data
        return {
            "order_id":           d.order_id,
            "region":             d.region,
            "country":            d.country,
            "item_type":          d.item_type,
            "sales_channel":      d.sales_channel,
            "priority":           d.priority,
            "order_date":         d.order_date,
            "ship_date":          d.ship_date,
            "units_sold":         d.units_sold,
            "unit_price":         d.unit_price,
            "unit_cost":          d.unit_cost,
            "total_revenue":      d.total_revenue,
            "total_cost":         d.total_cost,
            "total_profit":       d.total_profit,
            "kafka_topic":        record.kafka_topic,
            "kafka_partition":    record.kafka_partition,
            "kafka_offset":       record.kafka_offset,
            "kafka_timestamp_ms": record.kafka_timestamp_ms,
            "ingested_at":        record.ingested_at,
            "source_file":        record.source_file,
        }

    def append(self, record: BronzeRecord) -> None:
        self._buffer.append(record)

    @storage_retry
    def flush(self) -> None:
        if not self._buffer:
            return
        import pyarrow as pa
        rows = [self._to_row(r) for r in self._buffer]
        arrow_table = pa.Table.from_pylist(rows)
        try:
            self._table.append(arrow_table)
        except Exception as exc:
            raise IcebergWriteError("sales_bronze", cause=exc) from exc
        finally:
            count = len(self._buffer)
            self._buffer.clear()
            if count:
                logger.info("Flushed %d bronze records to Iceberg/S3", count)


class CloudSilverWriter:
    """
    Merges SilverRecord into the real Iceberg silver table in S3 + Glue.

    Deduplication strategy: the table is scanned once on startup to load
    existing `source_kafka_offset` values into memory. Subsequent writes
    skip already-seen offsets. This mirrors an Iceberg MERGE INTO on
    source_kafka_offset and is safe under at-least-once Kafka delivery.

    Partitioned by (order_year, order_month) via identity transforms.
    """

    def __init__(self, settings: PipelineSettings, *, strict: bool = False) -> None:
        import pyarrow as pa  # noqa: F401
        self._settings = settings
        self._strict = strict
        self._catalog = _glue_catalog(settings)
        self._table = self._get_or_create_table()
        self._seen: set[str] = self._load_existing_keys()
        self._buffer: list[SilverRecord] = []

    def _get_or_create_table(self) -> Any:
        from pyiceberg.exceptions import NoSuchTableError
        schema = _build_silver_schema()
        table_id = (self._settings.glue_database, "sales_silver")
        try:
            return self._catalog.load_table(table_id)
        except NoSuchTableError:
            logger.info("Creating Iceberg table %s", table_id)
            return self._catalog.create_table(
                table_id,
                schema=schema,
                location=self._settings.silver_path,
                partition_spec=_build_silver_partition_spec(schema),
                properties={
                    "format-version": "2",
                    "write.parquet.compression-codec": "zstd",
                },
            )

    def _load_existing_keys(self) -> set[str]:
        """Scan the Iceberg table to pre-populate the dedup set."""
        try:
            scan = self._table.scan(selected_fields=("source_kafka_offset",))
            arrow = scan.to_arrow()
            if len(arrow) == 0:
                return set()
            return set(arrow.column("source_kafka_offset").to_pylist())
        except Exception:
            logger.warning("Could not pre-load silver dedup keys — starting empty", exc_info=True)
            return set()

    def merge(self, record: SilverRecord) -> bool:
        if record.source_kafka_offset in self._seen:
            if self._strict:
                raise DuplicateRecordError(record.source_kafka_offset)
            logger.debug("Duplicate skipped: %s", record.source_kafka_offset)
            return False
        self._seen.add(record.source_kafka_offset)
        self._buffer.append(record)
        return True

    @staticmethod
    def _to_row(record: SilverRecord) -> dict[str, Any]:
        return {
            "order_id":              record.order_id,
            "region":                record.region,
            "country":               record.country,
            "item_type":             record.item_type,
            "sales_channel":         record.sales_channel,
            "priority":              record.priority,
            "order_date":            record.order_date,
            "ship_date":             record.ship_date,
            "order_year":            record.order_year,
            "order_month":           record.order_month,
            "lead_time_days":        record.lead_time_days,
            "units_sold":            record.units_sold,
            "unit_price":            record.unit_price,
            "unit_cost":             record.unit_cost,
            "total_revenue":         record.total_revenue,
            "total_cost":            record.total_cost,
            "total_profit":          record.total_profit,
            "margin_pct":            record.margin_pct,
            "bronze_ingested_at":    record.bronze_ingested_at,
            "silver_transformed_at": record.silver_transformed_at,
            "source_kafka_offset":   record.source_kafka_offset,
        }

    @storage_retry
    def flush(self) -> None:
        if not self._buffer:
            return
        import pyarrow as pa
        rows = [self._to_row(r) for r in self._buffer]
        # Decimal fields need explicit PyArrow types to avoid precision loss
        arrow_table = pa.Table.from_pylist(rows, schema=self._arrow_schema())
        try:
            self._table.append(arrow_table)
        except Exception as exc:
            raise IcebergWriteError("sales_silver", cause=exc) from exc
        finally:
            count = len(self._buffer)
            self._buffer.clear()
            if count:
                logger.info("Flushed %d silver records to Iceberg/S3", count)

    @staticmethod
    def _arrow_schema() -> Any:
        import pyarrow as pa
        return pa.schema([
            pa.field("order_id",              pa.int64()),
            pa.field("region",                pa.string()),
            pa.field("country",               pa.string()),
            pa.field("item_type",             pa.string()),
            pa.field("sales_channel",         pa.string()),
            pa.field("priority",              pa.string()),
            pa.field("order_date",            pa.date32()),
            pa.field("ship_date",             pa.date32()),
            pa.field("order_year",            pa.int32()),
            pa.field("order_month",           pa.int32()),
            pa.field("lead_time_days",        pa.int32()),
            pa.field("units_sold",            pa.int32()),
            pa.field("unit_price",            pa.decimal128(18, 4)),
            pa.field("unit_cost",             pa.decimal128(18, 4)),
            pa.field("total_revenue",         pa.decimal128(18, 4)),
            pa.field("total_cost",            pa.decimal128(18, 4)),
            pa.field("total_profit",          pa.decimal128(18, 4)),
            pa.field("margin_pct",            pa.decimal128(6, 2)),
            pa.field("bronze_ingested_at",    pa.timestamp("us", tz="UTC")),
            pa.field("silver_transformed_at", pa.timestamp("us", tz="UTC")),
            pa.field("source_kafka_offset",   pa.string()),
        ])


# ═══════════════════════════════════════════════════════════════════════════ #
# Factories — dispatch on settings.is_cloud                                   #
# ═══════════════════════════════════════════════════════════════════════════ #


def get_bronze_writer(
    settings: PipelineSettings,
) -> LocalBronzeWriter | CloudBronzeWriter:
    """Return the appropriate BronzeWriter for the current environment."""
    if settings.is_cloud:
        return CloudBronzeWriter(settings)
    return LocalBronzeWriter(settings)


def get_silver_writer(
    settings: PipelineSettings,
    *,
    strict: bool = False,
) -> LocalSilverWriter | CloudSilverWriter:
    """Return the appropriate SilverWriter for the current environment."""
    if settings.is_cloud:
        return CloudSilverWriter(settings, strict=strict)
    return LocalSilverWriter(settings, strict=strict)
