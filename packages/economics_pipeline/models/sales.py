from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from ..exceptions.validation import InvalidDateError, InvalidRecordError, SchemaError


class SalesRecord(BaseModel):
    """Raw sales record parsed from the source CSV, all fields typed."""

    model_config = ConfigDict(populate_by_name=True)

    order_id: int
    region: str
    country: str
    item_type: str
    sales_channel: str
    priority: str
    order_date: date
    ship_date: date
    units_sold: int
    unit_price: float
    unit_cost: float
    total_revenue: float
    total_cost: float
    total_profit: float

    @field_validator("order_date", "ship_date", mode="before")
    @classmethod
    def _parse_date(cls, v: Any) -> date:
        if isinstance(v, date):
            return v
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(v), fmt).date()
            except ValueError:
                continue
        raise InvalidDateError(str(v))

    @field_validator("ship_date", mode="after")
    @classmethod
    def _ship_after_order(cls, ship: date, info: Any) -> date:
        order = info.data.get("order_date")
        if order and ship < order:
            raise SchemaError(f"ship_date {ship} is before order_date {order}")
        return ship

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "SalesRecord":
        try:
            return cls(
                order_id=row["Order ID"],
                region=row["Region"],
                country=row["Country"],
                item_type=row["Item Type"],
                sales_channel=row["Sales Channel"],
                priority=row["Order Priority"],
                order_date=row["Order Date"],
                ship_date=row["Ship Date"],
                units_sold=row["Units Sold"],
                unit_price=row["Unit Price"],
                unit_cost=row["Unit Cost"],
                total_revenue=row["Total Revenue"],
                total_cost=row["Total Cost"],
                total_profit=row["Total Profit"],
            )
        except KeyError as exc:
            raise InvalidRecordError(
                field=str(exc).strip("'"),
                value=None,
                reason="required column missing from CSV row",
            ) from exc


class BronzeRecord(BaseModel):
    """SalesRecord enriched with Kafka ingestion metadata for the bronze layer."""

    data: SalesRecord
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp_ms: int
    ingested_at: datetime
    source_file: str

    @property
    def record_key(self) -> str:
        """Unique deduplication key: topic-partition-offset."""
        return f"{self.kafka_topic}-{self.kafka_partition}-{self.kafka_offset}"


class SilverRecord(BaseModel):
    """
    Curated, fully-typed, deduplicated sales record for the silver layer.

    Derived fields (order_year, order_month, lead_time_days, margin_pct) are
    computed at transformation time so downstream queries stay simple.
    Financial fields use Decimal to avoid IEEE-754 rounding in aggregations.
    """

    order_id: int
    region: str
    country: str
    item_type: str
    sales_channel: str
    priority: str
    order_date: date
    ship_date: date
    # Derived fields — computed at transform time, stored for query performance
    order_year: int       # Iceberg partition key (IdentityTransform)
    order_month: int      # Iceberg partition key (IdentityTransform)
    lead_time_days: int   # ship_date - order_date in days
    units_sold: int
    # Decimal for financials — avoids float rounding in downstream Athena sums
    unit_price: Decimal
    unit_cost: Decimal
    total_revenue: Decimal
    total_cost: Decimal
    total_profit: Decimal
    margin_pct: Decimal
    # Audit trail
    bronze_ingested_at: datetime
    silver_transformed_at: datetime
    source_kafka_offset: str  # "topic-partition-offset", the dedup key
