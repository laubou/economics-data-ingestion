"""
S3 read-only DAO.

Responsibility: stream records from the landing zone.
Never writes or deletes — safe to grant with read-only IAM policies.
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Iterator

import boto3

from ..config.base import PipelineSettings
from ..exceptions.storage import StorageError
from ..models.sales import SalesRecord
from ..retry.policy import network_retry

logger = logging.getLogger(__name__)


class S3LandingReader:
    """Reads CSV records from the landing zone on S3."""

    def __init__(self, settings: PipelineSettings) -> None:
        self._bucket = settings.s3_bucket
        self._client = boto3.client("s3", region_name=settings.aws_region)

    @network_retry
    def read_csv(self, filename: str) -> Iterator[SalesRecord]:
        key = f"landing/{filename}"
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            raw = response["Body"].read().decode("utf-8")
        except Exception as exc:
            raise StorageError(f"Failed to read s3://{self._bucket}/{key}") from exc
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            yield SalesRecord.from_csv_row(row)


class LocalLandingReader:
    """
    Reads CSV records from the local landing zone.
    Used in dev and unit tests — no AWS credentials needed.
    """

    def __init__(self, settings: PipelineSettings) -> None:
        self._landing_path = settings.landing_path

    def read_csv(self, filename: str) -> Iterator[SalesRecord]:
        filepath = f"{self._landing_path}/{filename}"
        with open(filepath, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                yield SalesRecord.from_csv_row(row)


def get_landing_reader(
    settings: PipelineSettings,
) -> S3LandingReader | LocalLandingReader:
    """Factory: S3 in cloud environments, local filesystem in dev."""
    if settings.is_cloud:
        return S3LandingReader(settings)
    return LocalLandingReader(settings)
