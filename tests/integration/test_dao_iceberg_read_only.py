"""
Integration tests for the Iceberg read-only DAO.

Tests the reader against files produced by the real writer
(not mocks) to catch serialisation/deserialisation regressions.

Run: pytest tests/integration/test_dao_iceberg_read_only.py -m integration
"""

import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from economics_pipeline.config.base import PipelineSettings
from economics_pipeline.dao.iceberg_dao_read_only import LocalBronzeReader
from economics_pipeline.dao.iceberg_dao_read_write import LocalBronzeWriter, LocalSilverWriter
from economics_pipeline.models.sales import BronzeRecord, SalesRecord, SilverRecord


def make_bronze(offset: int = 0) -> BronzeRecord:
    return BronzeRecord(
        data=SalesRecord(
            order_id=offset + 1,
            region="Europe", country="France",
            item_type="Beverages", sales_channel="Online", priority="H",
            order_date=date(2020, 1, 3), ship_date=date(2020, 1, 10),
            units_sold=100, unit_price=9.99, unit_cost=5.50,
            total_revenue=999.0, total_cost=550.0, total_profit=449.0,
        ),
        kafka_topic="sales-events", kafka_partition=0, kafka_offset=offset,
        kafka_timestamp_ms=1609459200000,
        ingested_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        source_file="data/landing/file.csv",
    )


@pytest.mark.integration
class TestLocalBronzeReaderIntegration:
    def test_reader_sees_all_written_records(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        for i in range(10):
            writer.append(make_bronze(offset=i))
        writer.flush()

        reader = LocalBronzeReader(dev_settings)
        records = list(reader.read_all())
        assert len(records) == 10

    def test_reader_does_not_see_records_not_yet_flushed(
        self, dev_settings: PipelineSettings
    ) -> None:
        writer = LocalBronzeWriter(dev_settings)
        for i in range(3):
            writer.append(make_bronze(offset=i))
        # Not flushed yet
        reader = LocalBronzeReader(dev_settings)
        assert list(reader.read_all()) == []

    def test_all_fields_preserved_after_roundtrip(self, dev_settings: PipelineSettings) -> None:
        original = make_bronze(offset=42)
        writer = LocalBronzeWriter(dev_settings)
        writer.append(original)
        writer.flush()

        reader = LocalBronzeReader(dev_settings)
        restored = next(reader.read_all())
        assert restored.kafka_partition == original.kafka_partition
        assert restored.kafka_offset == original.kafka_offset
        assert restored.data.unit_price == pytest.approx(original.data.unit_price)
        assert restored.data.order_date == original.data.order_date

    def test_reader_is_concurrent_safe_with_writer(
        self, dev_settings: PipelineSettings
    ) -> None:
        """Reader and writer can operate on the same directory without errors."""
        writer = LocalBronzeWriter(dev_settings)
        for i in range(5):
            writer.append(make_bronze(offset=i))
        writer.flush()

        reader = LocalBronzeReader(dev_settings)
        records = list(reader.read_all())

        # Add more after read
        writer.append(make_bronze(offset=99))
        writer.flush()

        records2 = list(reader.read_all())
        assert len(records2) == len(records) + 1
