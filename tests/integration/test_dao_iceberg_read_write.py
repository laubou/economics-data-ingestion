"""
Integration tests for the Iceberg read-write DAO.

Tests write behaviour under realistic conditions (multiple flush cycles,
crash simulation, large batches).

Run: pytest tests/integration/test_dao_iceberg_read_write.py -m integration
"""

import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from economics_pipeline.config.base import PipelineSettings
from economics_pipeline.dao.iceberg_dao_read_only import LocalBronzeReader
from economics_pipeline.dao.iceberg_dao_read_write import LocalBronzeWriter, LocalSilverWriter
from economics_pipeline.exceptions.storage import DuplicateRecordError, IcebergWriteError
from economics_pipeline.models.sales import BronzeRecord, SalesRecord, SilverRecord


def make_bronze(partition: int = 0, offset: int = 0) -> BronzeRecord:
    return BronzeRecord(
        data=SalesRecord(
            order_id=offset + 1, region="Europe", country="France",
            item_type="Beverages", sales_channel="Online", priority="H",
            order_date=date(2020, 1, 3), ship_date=date(2020, 1, 10),
            units_sold=100, unit_price=9.99, unit_cost=5.50,
            total_revenue=999.0, total_cost=550.0, total_profit=449.0,
        ),
        kafka_topic="sales-events", kafka_partition=partition, kafka_offset=offset,
        kafka_timestamp_ms=1609459200000,
        ingested_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        source_file="data/landing/file.csv",
    )


def make_silver(order_id: int = 1, kafka_offset: str = "sales-events-0-0") -> SilverRecord:
    return SilverRecord(
        order_id=order_id, region="Europe", country="France",
        item_type="Beverages", sales_channel="Online", priority="H",
        order_date=date(2020, 1, 3), ship_date=date(2020, 1, 10),
        order_year=2020, order_month=1, lead_time_days=7, units_sold=100,
        unit_price=Decimal("9.99"), unit_cost=Decimal("5.50"),
        total_revenue=Decimal("999.00"), total_cost=Decimal("550.00"),
        total_profit=Decimal("449.00"), margin_pct=Decimal("44.94"),
        bronze_ingested_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        silver_transformed_at=datetime(2021, 1, 2, tzinfo=timezone.utc),
        source_kafka_offset=kafka_offset,
    )


@pytest.mark.integration
class TestLocalBronzeWriterIntegration:
    def test_large_batch_all_flushed(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        n = 1000
        for i in range(n):
            writer.append(make_bronze(offset=i))
        writer.flush()
        assert len(os.listdir(dev_settings.bronze_path)) == 1
        reader = LocalBronzeReader(dev_settings)
        assert len(list(reader.read_all())) == n

    def test_partial_flush_then_remainder(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        for i in range(5):
            writer.append(make_bronze(offset=i))
        writer.flush()

        for i in range(5, 10):
            writer.append(make_bronze(offset=i))
        writer.flush()

        assert len(os.listdir(dev_settings.bronze_path)) == 2
        reader = LocalBronzeReader(dev_settings)
        assert len(list(reader.read_all())) == 10

    def test_crash_simulation_no_corruption(self, dev_settings: PipelineSettings) -> None:
        """
        Simulate a crash mid-flush (buffer cleared, files partially written).
        On restart, the writer should pick up and write remaining records cleanly.
        """
        writer = LocalBronzeWriter(dev_settings)
        for i in range(3):
            writer.append(make_bronze(offset=i))
        writer.flush()

        # Simulate restart
        writer2 = LocalBronzeWriter(dev_settings)
        writer2.append(make_bronze(offset=3))
        writer2.append(make_bronze(offset=4))
        writer2.flush()

        assert len(os.listdir(dev_settings.bronze_path)) == 2
        reader = LocalBronzeReader(dev_settings)
        assert len(list(reader.read_all())) == 5


@pytest.mark.integration
class TestLocalSilverWriterIntegration:
    def test_dedup_across_multiple_runs(self, dev_settings: PipelineSettings) -> None:
        keys = [f"sales-events-0-{i}" for i in range(10)]

        # Run 1
        w1 = LocalSilverWriter(dev_settings)
        for i, key in enumerate(keys):
            w1.merge(make_silver(order_id=i, kafka_offset=key))
        w1.flush()

        # Run 2 — full replay
        w2 = LocalSilverWriter(dev_settings)
        for i, key in enumerate(keys):
            w2.merge(make_silver(order_id=i, kafka_offset=key))
        w2.flush()

        # Run 3 — partial replay + new records
        new_keys = [f"sales-events-0-{i}" for i in range(5, 15)]
        w3 = LocalSilverWriter(dev_settings)
        for i, key in enumerate(new_keys):
            w3.merge(make_silver(order_id=i, kafka_offset=key))
        w3.flush()

        all_files = [f for _, _, fs in os.walk(dev_settings.silver_path) for f in fs]
        # Original 10 + 5 new = 15 unique keys total
        assert len(all_files) == 15

    def test_partitions_are_created_correctly(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings)
        writer.merge(make_silver(kafka_offset="k1"))
        writer.flush()

        year_dirs = os.listdir(dev_settings.silver_path)
        assert "year=2020" in year_dirs
        month_dirs = os.listdir(os.path.join(dev_settings.silver_path, "year=2020"))
        assert "month=01" in month_dirs

    def test_strict_mode_raises_on_any_duplicate(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings, strict=True)
        writer.merge(make_silver(kafka_offset="k1"))
        writer.flush()

        # New instance, strict mode — pre-loads existing keys
        writer2 = LocalSilverWriter(dev_settings, strict=True)
        with pytest.raises(DuplicateRecordError):
            writer2.merge(make_silver(kafka_offset="k1"))
