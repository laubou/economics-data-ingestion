"""Unit tests for the custom exception hierarchy."""

import pytest

from economics_pipeline.exceptions import (
    ConsumerError,
    DownloadError,
    DuplicateRecordError,
    ExtractionError,
    IcebergWriteError,
    InvalidDateError,
    InvalidRecordError,
    MaxRetriesExceededError,
    PipelineError,
    ProducerError,
    S3WriteError,
    SchemaError,
    StorageError,
    TopicNotFoundError,
    ValidationError,
)


class TestPipelineErrorHierarchy:
    def test_all_are_pipeline_errors(self) -> None:
        errors = [
            InvalidDateError("bad-date"),
            InvalidRecordError("field", "val", "reason"),
            SchemaError("bad schema"),
            DownloadError("http://example.com"),
            ExtractionError("/tmp/file.zip"),
            ProducerError("topic"),
            ConsumerError("topic"),
            TopicNotFoundError("topic"),
            DuplicateRecordError("key"),
            IcebergWriteError("table"),
            S3WriteError("s3://bucket/key"),
            MaxRetriesExceededError("op", 3),
        ]
        for err in errors:
            assert isinstance(err, PipelineError), f"{type(err).__name__} is not a PipelineError"

    def test_validation_errors_are_catchable_as_validation_error(self) -> None:
        for exc in [InvalidDateError("x"), InvalidRecordError("f", "v", "r"), SchemaError("s")]:
            assert isinstance(exc, ValidationError)

    def test_storage_errors_are_catchable_as_storage_error(self) -> None:
        for exc in [DuplicateRecordError("k"), IcebergWriteError("t"), S3WriteError("p")]:
            assert isinstance(exc, StorageError)

    def test_invalid_date_error_stores_value(self) -> None:
        exc = InvalidDateError("31-13-2020", field="order_date")
        assert exc.field == "order_date"
        assert exc.value == "31-13-2020"
        assert "31-13-2020" in str(exc)

    def test_invalid_record_error_stores_field_and_value(self) -> None:
        exc = InvalidRecordError("order_id", "abc", "not a number")
        assert exc.field == "order_id"
        assert exc.value == "abc"
        assert "order_id" in str(exc)

    def test_download_error_stores_url(self) -> None:
        exc = DownloadError("http://example.com/file.zip")
        assert exc.url == "http://example.com/file.zip"
        assert "http://example.com/file.zip" in str(exc)

    def test_download_error_with_cause(self) -> None:
        cause = ConnectionError("refused")
        exc = DownloadError("http://x.com", cause=cause)
        assert exc.cause is cause
        assert "caused by" in str(exc)

    def test_producer_error_stores_topic_and_order_id(self) -> None:
        exc = ProducerError("sales-events", order_id=42)
        assert exc.topic == "sales-events"
        assert exc.order_id == 42
        assert "42" in str(exc)

    def test_consumer_error_stores_location(self) -> None:
        exc = ConsumerError("topic", partition=2, offset=99)
        assert exc.partition == 2
        assert exc.offset == 99
        assert "partition=2" in str(exc)
        assert "offset=99" in str(exc)

    def test_duplicate_record_error_stores_key(self) -> None:
        exc = DuplicateRecordError("sales-events-0-42")
        assert exc.dedup_key == "sales-events-0-42"
        assert "sales-events-0-42" in str(exc)

    def test_max_retries_error_stores_operation_and_attempts(self) -> None:
        exc = MaxRetriesExceededError("download", attempts=3)
        assert exc.operation == "download"
        assert exc.attempts == 3
        assert "3 attempt" in str(exc)

    def test_max_retries_error_with_cause(self) -> None:
        cause = ConnectionError("timeout")
        exc = MaxRetriesExceededError("download", attempts=3, cause=cause)
        assert exc.cause is cause

    def test_iceberg_write_error_stores_table(self) -> None:
        exc = IcebergWriteError("sales_bronze")
        assert exc.table == "sales_bronze"

    def test_iceberg_write_error_with_cause(self) -> None:
        cause = OSError("disk full")
        exc = IcebergWriteError("sales_bronze", cause=cause)
        assert exc.cause is cause
        assert "caused by" in str(exc)

    def test_extraction_error_stores_archive_path(self) -> None:
        exc = ExtractionError("/data/archive/dataset.zip")
        assert exc.archive_path == "/data/archive/dataset.zip"
        assert "dataset.zip" in str(exc)

    def test_extraction_error_with_cause(self) -> None:
        import zipfile
        cause = zipfile.BadZipFile("not a zip")
        exc = ExtractionError("/data/archive/file.zip", cause=cause)
        assert exc.cause is cause

    def test_schema_error_is_validation_error(self) -> None:
        exc = SchemaError("ship_date 2020-01-01 is before order_date 2020-06-01")
        assert isinstance(exc, ValidationError)
        assert "ship_date" in str(exc)

    def test_topic_not_found_stores_topic(self) -> None:
        exc = TopicNotFoundError("sales-events")
        assert exc.topic == "sales-events"
        assert "sales-events" in str(exc)

    def test_kafka_errors_catchable_as_kafka_pipeline_error(self) -> None:
        from economics_pipeline.exceptions.kafka import KafkaPipelineError
        for exc in [ProducerError("topic"), ConsumerError("topic"), TopicNotFoundError("t")]:
            assert isinstance(exc, KafkaPipelineError)

    def test_ingestion_errors_catchable_as_ingestion_error(self) -> None:
        from economics_pipeline.exceptions.ingestion import IngestionError
        for exc in [DownloadError("http://x"), ExtractionError("/tmp/f")]:
            assert isinstance(exc, IngestionError)

    def test_s3_write_error_stores_path(self) -> None:
        exc = S3WriteError("s3://bucket/key")
        assert exc.path == "s3://bucket/key"
        assert "s3://bucket/key" in str(exc)

    def test_pipeline_error_without_cause_has_clean_str(self) -> None:
        exc = IcebergWriteError("table")
        assert "caused by" not in str(exc)

    def test_all_exceptions_can_be_caught_as_base_exception(self) -> None:
        exceptions = [
            InvalidDateError("x"),
            InvalidRecordError("f", "v", "r"),
            SchemaError("s"),
            DownloadError("http://x"),
            ExtractionError("/f"),
            ProducerError("t"),
            ConsumerError("t"),
            TopicNotFoundError("t"),
            DuplicateRecordError("k"),
            IcebergWriteError("tbl"),
            S3WriteError("s3://b/k"),
            MaxRetriesExceededError("op", 3),
        ]
        for exc in exceptions:
            try:
                raise exc
            except Exception:
                pass  # All must be catchable as plain Exception
