from __future__ import annotations
from .base import PipelineError


class StorageError(PipelineError):
    """Base for all lake-layer storage errors (S3, Iceberg, Glue)."""


class DuplicateRecordError(StorageError):
    """
    A record with the same deduplication key already exists in the silver layer.

    This is NOT an error condition in normal operation — the silver writer
    silently skips duplicates. It is raised explicitly only when the caller
    needs to know about the skip (e.g. in an audit or metrics context).
    """

    def __init__(self, dedup_key: str) -> None:
        self.dedup_key = dedup_key
        super().__init__(f"Record already exists for key '{dedup_key}' — skipped")


class IcebergWriteError(StorageError):
    """
    Writing to an Iceberg table failed (S3 I/O, Glue catalog update, schema mismatch…).

    Raised by the bronze/silver writers after all retry attempts are exhausted.
    """

    def __init__(self, table: str, cause: BaseException | None = None) -> None:
        self.table = table
        super().__init__(f"Failed to write to Iceberg table '{table}'", cause=cause)


class S3WriteError(StorageError):
    """
    S3 PutObject / CopyObject failed (permissions, throttling, network…).

    Raised by S3 DAOs after all retry attempts are exhausted.
    """

    def __init__(self, path: str, cause: BaseException | None = None) -> None:
        self.path = path
        super().__init__(f"Failed to write to S3 path '{path}'", cause=cause)
