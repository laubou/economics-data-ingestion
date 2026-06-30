"""Unit tests for the Iceberg read-only DAO (LocalBronzeReader)."""

import json
import os
from datetime import date, datetime, timezone

import pytest

from economics_pipeline.config.base import PipelineSettings
from economics_pipeline.dao.iceberg_dao_read_only import LocalBronzeReader
from economics_pipeline.dao.iceberg_dao_read_write import LocalBronzeWriter
from economics_pipeline.models.sales import BronzeRecord, SalesRecord


def make_bronze(partition: int = 0, offset: int = 0) -> BronzeRecord:
    return BronzeRecord(
        data=SalesRecord(
            order_id=offset + 1,
            region="Europe",
            country="France",
            item_type="Beverages",
            sales_channel="Online",
            priority="H",
            order_date=date(2020, 1, 3),
            ship_date=date(2020, 1, 10),
            units_sold=100,
            unit_price=9.99,
            unit_cost=5.50,
            total_revenue=999.0,
            total_cost=550.0,
            total_profit=449.0,
        ),
        kafka_topic="sales-events",
        kafka_partition=partition,
        kafka_offset=offset,
        kafka_timestamp_ms=1609459200000,
        ingested_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        source_file="data/landing/file.csv",
    )


class TestLocalBronzeReader:
    def test_reads_all_written_records(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        for i in range(5):
            writer.append(make_bronze(offset=i))
        writer.flush()

        reader = LocalBronzeReader(dev_settings)
        records = list(reader.read_all())
        assert len(records) == 5

    def test_empty_directory_yields_nothing(self, dev_settings: PipelineSettings) -> None:
        os.makedirs(dev_settings.bronze_path, exist_ok=True)
        reader = LocalBronzeReader(dev_settings)
        assert list(reader.read_all()) == []

    def test_missing_directory_yields_nothing(self, dev_settings: PipelineSettings) -> None:
        reader = LocalBronzeReader(dev_settings)
        assert list(reader.read_all()) == []

    def test_non_ndjson_files_are_skipped(self, dev_settings: PipelineSettings) -> None:
        os.makedirs(dev_settings.bronze_path, exist_ok=True)
        open(os.path.join(dev_settings.bronze_path, "ignore.txt"), "w").close()

        writer = LocalBronzeWriter(dev_settings)
        writer.append(make_bronze(offset=1))
        writer.flush()

        reader = LocalBronzeReader(dev_settings)
        records = list(reader.read_all())
        assert len(records) == 1

    def test_record_roundtrips_with_correct_key(self, dev_settings: PipelineSettings) -> None:
        original = make_bronze(partition=2, offset=99)
        writer = LocalBronzeWriter(dev_settings)
        writer.append(original)
        writer.flush()

        reader = LocalBronzeReader(dev_settings)
        restored = next(reader.read_all())
        assert restored.record_key == original.record_key
        assert restored.data.order_id == original.data.order_id

    def test_records_read_in_sorted_filename_order(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        # Write in reverse order
        for offset in [9, 3, 1, 7]:
            writer.append(make_bronze(offset=offset))
        writer.flush()

        reader = LocalBronzeReader(dev_settings)
        offsets = [r.kafka_offset for r in reader.read_all()]
        assert offsets == sorted(offsets)
