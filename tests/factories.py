"""
Test data factories — generate randomized but valid records.

Why random instead of hardcoded?
  • Multiple records can be created without key collisions (order_id, kafka_offset).
  • Tests that use random data prove the code works on ANY valid input, not just
    the one value the author happened to type.
  • Edge cases are still pinnable via keyword overrides.

Usage:
    from tests.factories import make_csv_row, make_bronze_record, make_silver_record

    row    = make_csv_row()                          # fully random
    row    = make_csv_row(**{"Order ID": "999"})     # pin one field
    bronze = make_bronze_record()                    # unique offset every call
    bronze = make_bronze_record(offset=42)           # pin offset for dedup tests
    silver = make_silver_record()                    # unique kafka offset every call
    silver = make_silver_record(kafka_offset="t-0-1")# pin for dedup tests
"""

from __future__ import annotations

import random
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from economics_pipeline.models.sales import BronzeRecord, SalesRecord, SilverRecord

# ─── Reference data ─────────────────────────────────────────────────────────

_REGIONS = [
    "Europe",
    "Asia",
    "North America",
    "Sub-Saharan Africa",
    "Middle East and North Africa",
    "Central America and the Caribbean",
    "Australia and Oceania",
]
_COUNTRIES: dict[str, list[str]] = {
    "Europe":                           ["France", "Germany", "United Kingdom", "Spain", "Italy", "Netherlands", "Sweden"],
    "Asia":                             ["Japan", "China", "India", "South Korea", "Vietnam", "Thailand"],
    "North America":                    ["United States of America", "Canada", "Mexico"],
    "Sub-Saharan Africa":               ["Kenya", "Nigeria", "South Africa", "Ghana", "Tanzania"],
    "Middle East and North Africa":     ["Egypt", "United Arab Emirates", "Morocco", "Saudi Arabia"],
    "Central America and the Caribbean":["Cuba", "Guatemala", "Honduras"],
    "Australia and Oceania":            ["Australia", "New Zealand", "Papua New Guinea"],
}
_ITEM_TYPES = [
    "Beverages", "Baby Food", "Clothes", "Cosmetics",
    "Fruits", "Meat", "Office Supplies", "Snacks",
    "Household", "Personal Care",
]
_CHANNELS = ["Online", "Offline"]
_PRIORITIES = ["H", "M", "L", "C"]

_DATE_RANGE_START = date(2015, 1, 1)
_DATE_RANGE_END   = date(2023, 12, 31)
_DATE_RANGE_DAYS  = (_DATE_RANGE_END - _DATE_RANGE_START).days


# ─── Primitives ─────────────────────────────────────────────────────────────

def unique_int() -> int:
    """Return a collision-resistant positive 31-bit integer (safe for DB IDs)."""
    return uuid.uuid4().int & 0x7FFF_FFFF


def unique_offset_key(topic: str = "test-sales-events", partition: int | None = None) -> str:
    """Return a unique Kafka offset string, e.g. 'test-sales-events-1-83924711'."""
    p = partition if partition is not None else random.randint(0, 2)
    return f"{topic}-{p}-{unique_int()}"


def _fmt_date(d: date) -> str:
    """M/D/YYYY with no leading zeros — matches the source CSV format."""
    return f"{d.month}/{d.day}/{d.year}"


# ─── CSV row factory ─────────────────────────────────────────────────────────

def make_csv_row(**overrides: str) -> dict[str, str]:
    """
    Return a dict[str, str] representing one CSV row with randomized valid data.

    All 14 source columns are present. Override any field via keyword arguments
    to pin specific values while keeping the rest random:

        make_csv_row(**{"Order Date": "not-a-date"})   # test invalid date
        make_csv_row(**{"Order ID": "42"})              # pin order_id
    """
    region  = random.choice(_REGIONS)
    country = random.choice(_COUNTRIES[region])
    order_date = _DATE_RANGE_START + timedelta(days=random.randint(0, _DATE_RANGE_DAYS))
    ship_date  = order_date + timedelta(days=random.randint(1, 45))
    units      = random.randint(1, 10_000)
    unit_price = round(random.uniform(1.0, 500.0), 2)
    unit_cost  = round(unit_price * random.uniform(0.20, 0.85), 2)

    base: dict[str, str] = {
        "Order ID":       str(unique_int()),
        "Region":         region,
        "Country":        country,
        "Item Type":      random.choice(_ITEM_TYPES),
        "Sales Channel":  random.choice(_CHANNELS),
        "Order Priority": random.choice(_PRIORITIES),
        "Order Date":     _fmt_date(order_date),
        "Ship Date":      _fmt_date(ship_date),
        "Units Sold":     str(units),
        "Unit Price":     str(unit_price),
        "Unit Cost":      str(unit_cost),
        "Total Revenue":  str(round(units * unit_price, 2)),
        "Total Cost":     str(round(units * unit_cost, 2)),
        "Total Profit":   str(round(units * (unit_price - unit_cost), 2)),
    }
    return {**base, **overrides}


# ─── Model factories ─────────────────────────────────────────────────────────

def make_sales_record(**overrides: str) -> SalesRecord:
    """Return a SalesRecord built from a random CSV row. Pin fields via overrides."""
    return SalesRecord.from_csv_row(make_csv_row(**overrides))


def make_bronze_record(
    *,
    partition: int | None = None,
    offset: int | None = None,
    topic: str = "test-sales-events",
    **sales_overrides: str,
) -> BronzeRecord:
    """
    Return a BronzeRecord with unique kafka_offset by default.

    Every call produces a different offset so records don't accidentally
    collide in dedup tests. Pin offset= when you WANT the same record twice:

        r1 = make_bronze_record(offset=42)
        r2 = make_bronze_record(offset=42)  # same logical record — tests idempotency
    """
    p = partition if partition is not None else random.randint(0, 2)
    o = offset if offset is not None else unique_int()
    sales = make_sales_record(**sales_overrides) if sales_overrides else make_sales_record()
    return BronzeRecord(
        data=sales,
        kafka_topic=topic,
        kafka_partition=p,
        kafka_offset=o,
        kafka_timestamp_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
        ingested_at=datetime.now(timezone.utc),
        source_file=f"data/landing/dataset_{uuid.uuid4().hex[:8]}.csv",
    )


def make_silver_record(
    *,
    order_id: int | None = None,
    kafka_offset: str | None = None,
    order_year: int = 2020,
    order_month: int = 1,
) -> SilverRecord:
    """
    Return a SilverRecord with unique source_kafka_offset by default.

    Pin kafka_offset= when testing deduplication:

        s1 = make_silver_record(kafka_offset="t-0-1")
        s2 = make_silver_record(kafka_offset="t-0-1")  # duplicate
    """
    oid    = order_id    if order_id    is not None else unique_int()
    offset = kafka_offset if kafka_offset is not None else unique_offset_key()
    now    = datetime.now(timezone.utc)
    return SilverRecord(
        order_id=oid,
        region=random.choice(_REGIONS),
        country="France",
        item_type=random.choice(_ITEM_TYPES),
        sales_channel=random.choice(_CHANNELS),
        priority=random.choice(_PRIORITIES),
        order_date=date(order_year, order_month, 3),
        ship_date=date(order_year, order_month, 10),
        order_year=order_year,
        order_month=order_month,
        lead_time_days=7,
        units_sold=random.randint(1, 5_000),
        unit_price=Decimal("9.99"),
        unit_cost=Decimal("5.50"),
        total_revenue=Decimal("999.00"),
        total_cost=Decimal("550.00"),
        total_profit=Decimal("449.00"),
        margin_pct=Decimal("44.94"),
        bronze_ingested_at=now,
        silver_transformed_at=now,
        source_kafka_offset=offset,
    )
