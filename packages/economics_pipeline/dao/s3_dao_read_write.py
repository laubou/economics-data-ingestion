"""
S3 read-write DAO.

Responsibility: write raw files to landing and archive zones.
These operations are idempotent: re-uploading the same key is safe.
"""

from __future__ import annotations

import logging
import os
import shutil

import boto3

from ..config.base import PipelineSettings
from ..exceptions.storage import S3WriteError
from ..retry.policy import network_retry

logger = logging.getLogger(__name__)


class S3LandingWriter:
    """Writes and archives files in the S3 data lake."""

    def __init__(self, settings: PipelineSettings) -> None:
        self._bucket = settings.s3_bucket
        self._client = boto3.client("s3", region_name=settings.aws_region)

    @network_retry
    def write(self, data: bytes, filename: str) -> str:
        key = f"landing/{filename}"
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
        except Exception as exc:
            raise S3WriteError(f"s3://{self._bucket}/{key}", cause=exc) from exc
        path = f"s3://{self._bucket}/{key}"
        logger.info("Written %d bytes to %s", len(data), path)
        return path

    @network_retry
    def archive(self, source_key: str, archive_filename: str) -> str:
        dest_key = f"archive/{archive_filename}"
        try:
            self._client.copy_object(
                CopySource={"Bucket": self._bucket, "Key": source_key},
                Bucket=self._bucket,
                Key=dest_key,
            )
        except Exception as exc:
            raise S3WriteError(f"s3://{self._bucket}/{dest_key}", cause=exc) from exc
        path = f"s3://{self._bucket}/{dest_key}"
        logger.info("Archived %s → %s", source_key, path)
        return path


class LocalLandingWriter:
    """
    Writes files to the local landing and archive directories.
    Used in dev and tests — no AWS credentials needed.
    """

    def __init__(self, settings: PipelineSettings) -> None:
        self._settings = settings

    def write(self, data: bytes, filename: str) -> str:
        path = os.path.join(self._settings.landing_path, filename)
        os.makedirs(self._settings.landing_path, exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        logger.debug("Written %d bytes to %s", len(data), path)
        return path

    def archive(self, source_path: str, archive_filename: str) -> str:
        dest = os.path.join(self._settings.archive_path, archive_filename)
        os.makedirs(self._settings.archive_path, exist_ok=True)
        shutil.copy2(source_path, dest)
        logger.debug("Archived %s → %s", source_path, dest)
        return dest


def get_landing_writer(
    settings: PipelineSettings,
) -> S3LandingWriter | LocalLandingWriter:
    """Factory: S3 in cloud environments, local filesystem in dev."""
    if settings.is_cloud:
        return S3LandingWriter(settings)
    return LocalLandingWriter(settings)
