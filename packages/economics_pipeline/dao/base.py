"""
Protocol definitions (interfaces/traits) for all storage operations.

Using Protocol instead of ABC lets us swap implementations without inheritance —
the same duck-typing pattern you'd use traits for in Rust, or interfaces in Go.
Services depend only on these Protocols, making unit tests trivial to write
(pass any object that satisfies the Protocol, no mocking framework needed).
"""

from __future__ import annotations

from typing import Iterator, Protocol, runtime_checkable

from ..models.sales import BronzeRecord, SalesRecord, SilverRecord


@runtime_checkable
class LandingWriter(Protocol):
    def write(self, data: bytes, filename: str) -> str:
        """Write raw bytes to the landing zone. Returns the final path."""
        ...


@runtime_checkable
class ArchiveWriter(Protocol):
    def archive(self, source_path: str, archive_filename: str) -> str:
        """Move/copy a file to the archive zone. Returns the archive path."""
        ...


@runtime_checkable
class LandingReader(Protocol):
    def read_csv(self, filename: str) -> Iterator[SalesRecord]:
        """Stream SalesRecords from a CSV in the landing zone."""
        ...


@runtime_checkable
class BronzeWriter(Protocol):
    def append(self, record: BronzeRecord) -> None:
        """Buffer a bronze record."""
        ...

    def flush(self) -> None:
        """Persist the buffer."""
        ...


@runtime_checkable
class BronzeReader(Protocol):
    def read_all(self) -> Iterator[BronzeRecord]:
        """Stream all bronze records."""
        ...


@runtime_checkable
class SilverWriter(Protocol):
    def merge(self, record: SilverRecord) -> bool:
        """
        Upsert a silver record by its source_kafka_offset key.
        Returns True if the record was added, False if it was a duplicate.
        Idempotent: replaying the same offset is a no-op.
        """
        ...

    def flush(self) -> None:
        """Persist the buffer."""
        ...
