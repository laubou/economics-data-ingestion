"""Unit tests for the Iceberg read-write DAO (LocalBronzeWriter, LocalSilverWriter)."""

import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from economics_pipeline.config.base import PipelineSettings
from economics_pipeline.dao.iceberg_dao_read_write import LocalBronzeWriter, LocalSilverWriter
from economics_pipeline.exceptions.storage import DuplicateRecordError
from economics_pipeline.models.sales import BronzeRecord, SalesRecord, SilverRecord
from tests.factories import make_bronze_record, make_silver_record as _make_silver


from tests.factories import make_bronze_record, make_silver_record as _make_silver


def make_bronze(partition: int | None = None, offset: int | None = None) -> BronzeRecord:
    """Random BronzeRecord. Pin partition/offset for idempotency tests."""
    kwargs: dict = {}
    if partition is not None:
        kwargs["partition"] = partition
    if offset is not None:
        kwargs["offset"] = offset
    return make_bronze_record(**kwargs)


def make_silver(order_id: int | None = None,
                kafka_offset: str | None = None) -> SilverRecord:
    """Random SilverRecord. Pin kafka_offset to test dedup behaviour."""
    return _make_silver(order_id=order_id, kafka_offset=kafka_offset)


# ------------------------------------------------------------------ #
# LocalBronzeWriter                                                    #
# ------------------------------------------------------------------ #


class TestLocalBronzeWriter:
    def test_flush_creates_one_file_per_batch(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        writer.append(make_bronze(partition=0, offset=1))
        writer.append(make_bronze(partition=0, offset=2))
        writer.flush()
        assert len(os.listdir(dev_settings.bronze_path)) == 1

    def test_filename_encodes_partition_and_offset(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        writer.append(make_bronze(partition=2, offset=99))
        writer.flush()
        files = os.listdir(dev_settings.bronze_path)
        assert any("p2" in f and "o99" in f for f in files)

    def test_same_offset_overwrites_idempotently(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        writer.append(make_bronze(partition=0, offset=5))
        writer.flush()
        writer.append(make_bronze(partition=0, offset=5))
        writer.flush()
        assert len(os.listdir(dev_settings.bronze_path)) == 1

    def test_buffer_is_cleared_after_flush(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        writer.append(make_bronze())
        writer.flush()
        assert writer._buffer == []

    def test_multiple_flushes_accumulate_files(self, dev_settings: PipelineSettings) -> None:
        writer = LocalBronzeWriter(dev_settings)
        for i in range(3):
            writer.append(make_bronze(offset=i))
            writer.flush()
        assert len(os.listdir(dev_settings.bronze_path)) == 3

    def test_flush_raises_iceberg_write_error_on_os_error(
        self, dev_settings: PipelineSettings
    ) -> None:
        from unittest.mock import patch, mock_open
        from economics_pipeline.exceptions.storage import IcebergWriteError

        writer = LocalBronzeWriter(dev_settings)
        writer.append(make_bronze(offset=1))

        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(IcebergWriteError) as exc_info:
                writer.flush()
        assert exc_info.value.table == "sales_bronze"
        assert isinstance(exc_info.value.cause, OSError)


# ------------------------------------------------------------------ #
# LocalSilverWriter                                                    #
# ------------------------------------------------------------------ #


class TestLocalSilverWriter:
    def test_merge_writes_new_record(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings)
        writer.merge(make_silver(kafka_offset="sales-events-0-1"))
        writer.flush()
        all_files = [f for _, _, fs in os.walk(dev_settings.silver_path) for f in fs]
        assert len(all_files) == 1

    def test_merge_silently_skips_duplicate_by_default(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings)
        key = "sales-events-0-42"
        writer.merge(make_silver(kafka_offset=key))
        writer.merge(make_silver(kafka_offset=key))  # duplicate
        writer.flush()
        all_files = [f for _, _, fs in os.walk(dev_settings.silver_path) for f in fs]
        assert len(all_files) == 1

    def test_merge_raises_duplicate_error_in_strict_mode(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings, strict=True)
        key = "sales-events-0-1"
        writer.merge(make_silver(kafka_offset=key))
        with pytest.raises(DuplicateRecordError) as exc_info:
            writer.merge(make_silver(kafka_offset=key))
        assert exc_info.value.dedup_key == key

    def test_partitioned_by_year_month(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings)
        writer.merge(make_silver(kafka_offset="sales-events-0-1"))
        writer.flush()
        subdirs = [d for _, ds, _ in os.walk(dev_settings.silver_path) for d in ds]
        assert "year=2020" in subdirs

    def test_replay_idempotent_across_writer_instances(self, dev_settings: PipelineSettings) -> None:
        key = "sales-events-0-10"
        w1 = LocalSilverWriter(dev_settings)
        w1.merge(make_silver(kafka_offset=key))
        w1.flush()

        # New instance simulates a service restart
        w2 = LocalSilverWriter(dev_settings)
        w2.merge(make_silver(kafka_offset=key))
        w2.flush()

        all_files = [f for _, _, fs in os.walk(dev_settings.silver_path) for f in fs]
        assert len(all_files) == 1

    def test_buffer_cleared_after_flush(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings)
        writer.merge(make_silver())
        writer.flush()
        assert writer._buffer == []

    def test_multiple_unique_records_all_written(self, dev_settings: PipelineSettings) -> None:
        writer = LocalSilverWriter(dev_settings)
        for i in range(5):
            writer.merge(make_silver(order_id=i, kafka_offset=f"sales-events-0-{i}"))
        writer.flush()
        all_files = [f for _, _, fs in os.walk(dev_settings.silver_path) for f in fs]
        assert len(all_files) == 5

    def test_flush_raises_iceberg_write_error_on_os_error(
        self, dev_settings: PipelineSettings
    ) -> None:
        from unittest.mock import patch
        from economics_pipeline.exceptions.storage import IcebergWriteError

        writer = LocalSilverWriter(dev_settings)
        writer.merge(make_silver(kafka_offset="sales-events-0-1"))

        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(IcebergWriteError) as exc_info:
                writer.flush()
        assert exc_info.value.table == "sales_silver"
        assert isinstance(exc_info.value.cause, OSError)
