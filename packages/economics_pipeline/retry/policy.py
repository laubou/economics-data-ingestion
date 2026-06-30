"""
Retry policies using tenacity.

Three distinct policies cover the three failure domains in the pipeline:
  - network_retry   : HTTP downloads, S3 I/O
  - kafka_retry     : broker connections, produce/consume calls
  - storage_retry   : Iceberg / Glue catalog writes

Each policy wraps the tenacity RetryError in a domain-specific
MaxRetriesExceededError so callers never need to import tenacity.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import wraps
from typing import TypeVar

from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..exceptions.base import MaxRetriesExceededError

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)


def _wrap_retry_error(operation: str, attempts: int) -> Callable[[F], F]:
    """
    Decorator that catches tenacity RetryError and re-raises as
    MaxRetriesExceededError, keeping the original cause attached.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except RetryError as exc:
                cause = exc.last_attempt.exception()
                raise MaxRetriesExceededError(
                    operation=operation, attempts=attempts, cause=cause
                ) from exc

        return wrapper  # type: ignore[return-value]

    return decorator


# ------------------------------------------------------------------ #
# Network retry — HTTP downloads, S3 I/O                               #
# 3 attempts, 1 → 2 → 4 s backoff                                     #
# ------------------------------------------------------------------ #

_NETWORK_ATTEMPTS = 3


def network_retry(func: F) -> F:
    """
    Retry on transient network errors (connection refused, timeout, OS error).

    Covers:
      - requests.exceptions.ConnectionError / Timeout
      - botocore.exceptions.ClientError (S3 throttling maps to ConnectionError)
      - Generic OSError (DNS failures, socket timeouts)
    """
    retrying = retry(
        stop=stop_after_attempt(_NETWORK_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=False,
    )(func)

    @wraps(func)
    @_wrap_retry_error(func.__name__, _NETWORK_ATTEMPTS)
    def wrapper(*args, **kwargs):
        return retrying(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


# ------------------------------------------------------------------ #
# Kafka retry — broker connections, produce/consume                    #
# 5 attempts, 2 → 4 → 8 → 16 → 30 s backoff (capped at 30 s)         #
# ------------------------------------------------------------------ #

_KAFKA_ATTEMPTS = 5


def kafka_retry(func: F) -> F:
    """
    Retry on transient Kafka errors (NoBrokersAvailable, timeout, leader election).

    The longer backoff (up to 30 s) gives MSK time to recover after a
    broker restart or leader re-election without hammering the cluster.
    """
    # Import here to avoid hard-dependency when not using Kafka
    from kafka.errors import KafkaError  # type: ignore[import-untyped]

    retrying = retry(
        stop=stop_after_attempt(_KAFKA_ATTEMPTS),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((KafkaError, ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=False,
    )(func)

    @wraps(func)
    @_wrap_retry_error(func.__name__, _KAFKA_ATTEMPTS)
    def wrapper(*args, **kwargs):
        return retrying(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


# ------------------------------------------------------------------ #
# Storage retry — Iceberg writes, Glue catalog, S3 PutObject           #
# 3 attempts, 1 → 2 → 4 s backoff                                     #
# ------------------------------------------------------------------ #

_STORAGE_ATTEMPTS = 3


def storage_retry(func: F) -> F:
    """
    Retry on transient storage errors (S3 throttling, Glue conflict, I/O).

    Does NOT retry on DuplicateRecordError — duplicates are not transient.
    """
    from ..exceptions.storage import DuplicateRecordError

    retrying = retry(
        stop=stop_after_attempt(_STORAGE_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=False,
    )(func)

    @wraps(func)
    @_wrap_retry_error(func.__name__, _STORAGE_ATTEMPTS)
    def wrapper(*args, **kwargs):
        return retrying(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
