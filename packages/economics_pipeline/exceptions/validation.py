from __future__ import annotations
from .base import PipelineError


class ValidationError(PipelineError):
    """A record or field failed business validation."""


class InvalidRecordError(ValidationError):
    """
    A CSV row or Kafka message cannot be parsed into a SalesRecord.

    Raised by SalesRecord.from_csv_row() or BronzeConsumer when
    the Kafka message payload does not match the expected schema.
    """

    def __init__(self, field: str, value: object, reason: str) -> None:
        self.field = field
        self.value = value
        super().__init__(f"Invalid value for field '{field}': {value!r} — {reason}")


class InvalidDateError(InvalidRecordError):
    """
    A date string cannot be parsed into a Python date.

    Raised by the SalesRecord date validator when neither
    '%m/%d/%Y' nor '%Y-%m-%d' matches the incoming value.
    """

    def __init__(self, value: str, field: str = "date") -> None:
        super().__init__(
            field=field,
            value=value,
            reason=f"expected format MM/DD/YYYY or YYYY-MM-DD, got {value!r}",
        )


class SchemaError(ValidationError):
    """
    A record is structurally valid but violates a business rule
    (e.g. ship_date before order_date, negative revenue).
    """
