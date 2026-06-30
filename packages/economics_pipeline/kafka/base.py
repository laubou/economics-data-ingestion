"""
Kafka Protocol definitions.

Separating the interface from the implementation lets unit tests inject
a fake producer/consumer without spinning up a real broker.
"""

from __future__ import annotations

from typing import Iterator, Protocol, runtime_checkable

from ..models.sales import BronzeRecord, SalesRecord


@runtime_checkable
class IProducer(Protocol):
    def send(self, record: SalesRecord) -> object:
        """Enqueue a record. Returns a Future-like object."""
        ...

    def flush(self) -> None:
        """Block until all enqueued records have been acknowledged."""
        ...

    def close(self) -> None: ...


@runtime_checkable
class IConsumer(Protocol):
    def consume(self) -> Iterator[BronzeRecord]:
        """Yield bronze records, committing offset after each successful yield."""
        ...

    def close(self) -> None: ...
