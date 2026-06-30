"""
Shared pytest fixtures.

The `dev_settings` fixture uses tmp_path so every test gets an isolated
directory — no test can pollute another's bronze/silver files.

`sample_csv_row` keeps hardcoded values for tests that assert specific field
values. For tests that just need *any* valid row, prefer `random_csv_row`.
"""

import pytest

from economics_pipeline.config.base import PipelineSettings
from tests.factories import make_csv_row


@pytest.fixture
def dev_settings(tmp_path: pytest.TempPathFactory) -> PipelineSettings:
    """Fully isolated settings pointing at a temporary directory."""
    return PipelineSettings(
        environment="test",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test-sales-events",
        kafka_group_id="test-bronze-group",
        data_base_path=str(tmp_path),
        s3_bucket="",
        max_rows=10,
    )


@pytest.fixture
def sample_csv_row() -> dict[str, str]:
    """Stable hardcoded row — use only when a test asserts specific field values."""
    return {
        "Order ID":       "123456",
        "Region":         "Europe",
        "Country":        "France",
        "Item Type":      "Beverages",
        "Sales Channel":  "Online",
        "Order Priority": "H",
        "Order Date":     "1/3/2010",
        "Ship Date":      "2/9/2010",
        "Units Sold":     "250",
        "Unit Price":     "9.99",
        "Unit Cost":      "5.50",
        "Total Revenue":  "2497.50",
        "Total Cost":     "1375.00",
        "Total Profit":   "1122.50",
    }


@pytest.fixture
def random_csv_row() -> dict[str, str]:
    """Randomized valid CSV row — use when a test only needs *some* valid input."""
    return make_csv_row()
