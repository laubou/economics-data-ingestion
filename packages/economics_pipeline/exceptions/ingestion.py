from __future__ import annotations
from .base import PipelineError


class IngestionError(PipelineError):
    """Base for all errors in the acquire-and-land step."""


class DownloadError(IngestionError):
    """
    HTTP fetch of the source file failed.

    Raised after all retry attempts are exhausted.
    Wraps the underlying requests exception as `cause`.
    """

    def __init__(self, url: str, cause: BaseException | None = None) -> None:
        self.url = url
        super().__init__(f"Failed to download {url!r}", cause=cause)


class ExtractionError(IngestionError):
    """
    ZIP extraction failed (corrupt archive, I/O error, disk full…).

    Raised after the archive is verified present on disk.
    """

    def __init__(self, archive_path: str, cause: BaseException | None = None) -> None:
        self.archive_path = archive_path
        super().__init__(f"Failed to extract archive {archive_path!r}", cause=cause)


class LandingError(IngestionError):
    """
    Writing the extracted file to the landing zone failed.

    Raised on local I/O error or S3 write failure.
    """
