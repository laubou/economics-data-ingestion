"""
Downloader service — Step 1 of the pipeline.

Triggered on a daily schedule (09:00 UTC via EventBridge in AWS,
or the scheduler service locally).

Flow:
  1. HEAD {source_url}  — check ETag/Content-Length against last known state
  2. If provider has no new data → exit immediately (idempotent)
  3. GET  {source_url}  → save to archive/ (original zip preserved)
  4. Unzip             → land CSV under landing/
  5. Persist ingestion state → status "extracted", ready for producer

Retry: HTTP requests retried up to 3× with exponential backoff (via @network_retry).
On final failure DownloadError is raised and the service exits non-zero.
"""

from __future__ import annotations

import logging
import os
import zipfile
from datetime import datetime, timezone

import requests
from requests.exceptions import RequestException

from economics_pipeline.config import get_settings
from economics_pipeline.exceptions.ingestion import DownloadError, ExtractionError
from economics_pipeline.ingestion.state import FileIngestionState, IngestionStateManager
from economics_pipeline.retry.policy import network_retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _head(url: str) -> tuple[str | None, int | None]:
    """Return (ETag, Content-Length) from a HEAD request, or (None, None) on failure."""
    try:
        resp = requests.head(url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        etag = resp.headers.get("ETag")
        raw_len = resp.headers.get("Content-Length")
        content_length = int(raw_len) if raw_len else None
        return etag, content_length
    except RequestException:
        # HEAD failure is non-fatal — fall through to download
        logger.warning("HEAD request failed for %s — will proceed with download", url)
        return None, None


@network_retry
def _download(url: str, dest: str) -> None:
    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=65_536):
                f.write(chunk)
    except RequestException as exc:
        raise DownloadError(url, cause=exc) from exc


def run() -> None:
    settings = get_settings()
    state_mgr = IngestionStateManager(settings.state_path)

    archive_dir = settings.archive_path
    landing_dir = settings.landing_path
    os.makedirs(archive_dir, exist_ok=True)
    os.makedirs(landing_dir, exist_ok=True)

    # ---- 1. Check if provider has new data ----
    etag, content_length = _head(settings.source_url)
    if not state_mgr.has_new_content(etag, content_length):
        logger.info(
            "No new data at %s (ETag=%s, size=%s) — nothing to do",
            settings.source_url, etag, content_length,
        )
        return

    archive_path = os.path.join(archive_dir, "dataset.zip")

    # ---- 2. Download ----
    logger.info("New data detected — downloading from %s", settings.source_url)
    _download(settings.source_url, archive_path)
    logger.info("Archived zip to %s", archive_path)

    # ---- 3. Extract → land ----
    landing_file = os.path.join(landing_dir, settings.source_filename)
    logger.info("Extracting to %s", landing_dir)
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(landing_dir)
    except (zipfile.BadZipFile, OSError) as exc:
        raise ExtractionError(archive_path, cause=exc) from exc

    logger.info("Landed: %s", landing_file)

    # ---- 4. Persist state (status "extracted" → producer will pick this up) ----
    state_mgr.save(FileIngestionState(
        url=settings.source_url,
        etag=etag,
        content_length=content_length,
        checksum_md5=IngestionStateManager.md5(archive_path),
        downloaded_at=datetime.now(timezone.utc).isoformat(),
        status="extracted",
    ))
    logger.info("Ingestion state updated — ready for producer")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.critical("Downloader failed: %s", exc, exc_info=True)
        raise SystemExit(1) from exc
