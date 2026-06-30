"""Unit tests for Pydantic models — no IO, no Kafka, no AWS."""

import pytest
from datetime import date, datetime, timezone

from economics_pipeline.models.sales import BronzeRecord, SalesRecord, SilverRecord
from decimal import Decimal

from tests.factories import make_bronze_record, make_csv_row


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def make_row(**overrides: str) -> dict[str, str]:
    """Random valid CSV row. Pin specific fields via overrides."""
    return make_csv_row(**overrides)


def make_bronze(order_id: int | None = None, partition: int | None = None,
                offset: int | None = None) -> BronzeRecord:
    """Random BronzeRecord. Pin partition/offset for dedup tests."""
    kwargs: dict = {}
    if partition is not None:
        kwargs["partition"] = partition
    if offset is not None:
        kwargs["offset"] = offset
    record = make_bronze_record(**kwargs)
    if order_id is not None:
        # Rebuild with the pinned order_id
        from tests.factories import make_sales_record
        sales = make_sales_record(**{"Order ID": str(order_id)})
        record = record.model_copy(update={"data": sales})
    return record


# ------------------------------------------------------------------ #
# SalesRecord                                                          #
# ------------------------------------------------------------------ #


class TestSalesRecord:
    def test_parse_from_csv_row(self, sample_csv_row: dict[str, str]) -> None:
        r = SalesRecord.from_csv_row(sample_csv_row)
        assert r.order_id == 123456
        assert r.region == "Europe"
        assert r.country == "France"
        assert r.order_date == date(2010, 1, 3)
        assert r.ship_date == date(2010, 2, 9)
        assert r.units_sold == 250
        assert r.unit_price == pytest.approx(9.99)

    def test_date_format_m_d_y(self) -> None:
        r = SalesRecord.from_csv_row(make_row(**{"Order Date": "12/31/2020", "Ship Date": "1/15/2021"}))
        assert r.order_date == date(2020, 12, 31)
        assert r.ship_date == date(2021, 1, 15)

    def test_date_format_iso(self) -> None:
        r = SalesRecord.from_csv_row(make_row(**{"Order Date": "2020-06-15", "Ship Date": "2020-06-20"}))
        assert r.order_date == date(2020, 6, 15)

    def test_invalid_date_raises_invalid_date_error(self) -> None:
        from economics_pipeline.exceptions.validation import InvalidDateError

        # Pydantic v2 passes non-ValueError exceptions through directly,
        # so callers receive InvalidDateError, not PydanticValidationError.
        with pytest.raises(InvalidDateError) as exc_info:
            SalesRecord.from_csv_row(make_row(**{"Order Date": "not-a-date"}))
        assert "not-a-date" in str(exc_info.value)

    def test_numeric_fields_coerced(self) -> None:
        r = SalesRecord.from_csv_row(make_row())
        assert isinstance(r.total_revenue, float)
        assert isinstance(r.units_sold, int)

    def test_missing_required_field_raises_invalid_record_error(self) -> None:
        from economics_pipeline.exceptions.validation import InvalidRecordError
        row = make_row()
        del row["Order ID"]
        with pytest.raises(InvalidRecordError) as exc_info:
            SalesRecord.from_csv_row(row)
        assert exc_info.value.field == "Order ID"
        assert "missing" in str(exc_info.value)

    def test_ship_date_before_order_date_raises_schema_error(self) -> None:
        from economics_pipeline.exceptions.validation import SchemaError

        # Same as InvalidDateError: Pydantic v2 passes SchemaError through directly.
        with pytest.raises(SchemaError) as exc_info:
            SalesRecord.from_csv_row(make_row(**{
                "Order Date": "6/1/2020",
                "Ship Date": "1/1/2020",  # before order date
            }))
        assert "ship_date" in str(exc_info.value)
        assert "order_date" in str(exc_info.value)


# ------------------------------------------------------------------ #
# BronzeRecord                                                         #
# ------------------------------------------------------------------ #


class TestBronzeRecord:
    def test_record_key_format(self) -> None:
        record = make_bronze(partition=2, offset=99)
        # Key format: {topic}-{partition}-{offset} — topic varies by test config
        assert record.record_key == f"{record.kafka_topic}-2-99"

    def test_record_key_is_unique_per_offset(self) -> None:
        r1 = make_bronze(partition=0, offset=1)
        r2 = make_bronze(partition=0, offset=2)
        assert r1.record_key != r2.record_key

    def test_record_key_is_unique_per_partition(self) -> None:
        r1 = make_bronze(partition=0, offset=5)
        r2 = make_bronze(partition=1, offset=5)
        assert r1.record_key != r2.record_key

    def test_roundtrip_serialisation(self) -> None:
        original = make_bronze()
        dumped = original.model_dump(mode="json")
        restored = BronzeRecord.model_validate(dumped)
        assert restored.record_key == original.record_key
        assert restored.data.order_id == original.data.order_id
