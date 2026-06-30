from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from ..models.sales import BronzeRecord, SilverRecord

_CENTS = Decimal("0.01")


def transform_to_silver(record: BronzeRecord) -> SilverRecord:
    """
    Pure function: BronzeRecord → SilverRecord.

    Pure functions are trivial to unit-test and parallelise. All side effects
    (reading, writing) are handled by the caller service.

    Dedup key (source_kafka_offset) is set here so the silver writer can
    apply MERGE INTO / upsert semantics without knowing Kafka internals.
    """
    data = record.data

    lead_time_days = (data.ship_date - data.order_date).days

    total_revenue = Decimal(str(data.total_revenue))
    total_cost = Decimal(str(data.total_cost))
    total_profit = Decimal(str(data.total_profit))

    margin_pct = (
        (total_profit / total_revenue * 100).quantize(_CENTS, rounding=ROUND_HALF_UP)
        if total_revenue
        else Decimal("0.00")
    )

    return SilverRecord(
        order_id=data.order_id,
        region=data.region,
        country=data.country,
        item_type=data.item_type,
        sales_channel=data.sales_channel,
        priority=data.priority,
        order_date=data.order_date,
        ship_date=data.ship_date,
        order_year=data.order_date.year,
        order_month=data.order_date.month,
        lead_time_days=lead_time_days,
        units_sold=data.units_sold,
        unit_price=Decimal(str(data.unit_price)),
        unit_cost=Decimal(str(data.unit_cost)),
        total_revenue=total_revenue,
        total_cost=total_cost,
        total_profit=total_profit,
        margin_pct=margin_pct,
        bronze_ingested_at=record.ingested_at,
        silver_transformed_at=datetime.now(timezone.utc),
        source_kafka_offset=record.record_key,
    )
