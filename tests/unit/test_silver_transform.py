"""Unit tests for the bronze → silver pure transform function."""

import pytest
from datetime import date, datetime, timezone
from decimal import Decimal

from economics_pipeline.models.sales import BronzeRecord, SalesRecord
from economics_pipeline.transforms.silver import transform_to_silver
from tests.factories import make_bronze_record, unique_int


def make_bronze(
    order_date: date = date(2020, 1, 3),
    ship_date: date = date(2020, 1, 10),
    total_revenue: float = 999.0,
    total_profit: float = 449.0,
    partition: int | None = None,
    offset: int | None = None,
) -> BronzeRecord:
    """
    Bronze record with pinnable date and financial fields for transform tests.
    Remaining fields (order_id, country, etc.) are randomized via the factory.
    """
    base = make_bronze_record(partition=partition, offset=offset)
    updated_data = base.data.model_copy(update={
        "order_date":    order_date,
        "ship_date":     ship_date,
        "total_revenue": total_revenue,
        "total_cost":    total_revenue - total_profit,
        "total_profit":  total_profit,
    })
    return base.model_copy(update={"data": updated_data})


class TestTransformToSilver:
    def test_lead_time_computed_correctly(self) -> None:
        silver = transform_to_silver(make_bronze(
            order_date=date(2020, 1, 3),
            ship_date=date(2020, 1, 10),
        ))
        assert silver.lead_time_days == 7

    def test_lead_time_zero_same_day(self) -> None:
        d = date(2020, 6, 1)
        silver = transform_to_silver(make_bronze(order_date=d, ship_date=d))
        assert silver.lead_time_days == 0

    def test_order_year_and_month_extracted(self) -> None:
        silver = transform_to_silver(make_bronze(order_date=date(2019, 11, 5)))
        assert silver.order_year == 2019
        assert silver.order_month == 11

    def test_margin_pct_calculation(self) -> None:
        silver = transform_to_silver(make_bronze(total_revenue=1000.0, total_profit=250.0))
        assert silver.margin_pct == Decimal("25.00")

    def test_margin_pct_zero_revenue(self) -> None:
        silver = transform_to_silver(make_bronze(total_revenue=0.0, total_profit=0.0))
        assert silver.margin_pct == Decimal("0.00")

    def test_financial_fields_are_decimal(self) -> None:
        silver = transform_to_silver(make_bronze())
        assert isinstance(silver.unit_price, Decimal)
        assert isinstance(silver.total_revenue, Decimal)
        assert isinstance(silver.total_profit, Decimal)
        assert isinstance(silver.margin_pct, Decimal)

    def test_source_kafka_offset_matches_bronze_key(self) -> None:
        bronze = make_bronze(partition=1, offset=99)
        silver = transform_to_silver(bronze)
        assert silver.source_kafka_offset == bronze.record_key
        # Format: {topic}-{partition}-{offset} — topic varies by environment
        assert silver.source_kafka_offset == f"{bronze.kafka_topic}-1-99"

    def test_bronze_audit_timestamp_preserved(self) -> None:
        bronze = make_bronze()
        silver = transform_to_silver(bronze)
        assert silver.bronze_ingested_at == bronze.ingested_at

    def test_silver_transform_timestamp_set(self) -> None:
        before = datetime.now(timezone.utc)
        silver = transform_to_silver(make_bronze())
        after = datetime.now(timezone.utc)
        assert before <= silver.silver_transformed_at <= after

    def test_passthrough_fields_unchanged(self) -> None:
        bronze = make_bronze()
        silver = transform_to_silver(bronze)
        assert silver.order_id == bronze.data.order_id
        assert silver.region == bronze.data.region
        assert silver.country == bronze.data.country
        assert silver.item_type == bronze.data.item_type
        assert silver.sales_channel == bronze.data.sales_channel
        assert silver.priority == bronze.data.priority


class TestMarginPctPrecision:
    """Decimal precision edge cases — floats would give wrong answers here."""

    @pytest.mark.parametrize("revenue,profit,expected_margin", [
        (100.0, 33.33, Decimal("33.33")),
        (333.33, 111.11, Decimal("33.33")),
        (1_000_000.0, 123_456.78, Decimal("12.35")),
    ])
    def test_margin_precision(
        self, revenue: float, profit: float, expected_margin: Decimal
    ) -> None:
        silver = transform_to_silver(make_bronze(total_revenue=revenue, total_profit=profit))
        assert silver.margin_pct == expected_margin
