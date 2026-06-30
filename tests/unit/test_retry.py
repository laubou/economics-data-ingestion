"""Unit tests for retry policies — no real network or Kafka needed."""

import pytest
from unittest.mock import patch, MagicMock, call

from economics_pipeline.exceptions.base import MaxRetriesExceededError
from economics_pipeline.retry.policy import network_retry, storage_retry


class TestNetworkRetry:
    def test_succeeds_on_first_attempt(self) -> None:
        call_count = 0

        @network_retry
        def flaky() -> str:
            nonlocal call_count
            call_count += 1
            return "ok"

        assert flaky() == "ok"
        assert call_count == 1

    def test_retries_on_connection_error(self) -> None:
        call_count = 0

        @network_retry
        def flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("refused")
            return "ok"

        assert flaky() == "ok"
        assert call_count == 3

    def test_raises_max_retries_after_all_attempts_fail(self) -> None:
        @network_retry
        def always_fails() -> None:
            raise ConnectionError("refused")

        with pytest.raises(MaxRetriesExceededError) as exc_info:
            always_fails()

        assert exc_info.value.attempts == 3
        assert exc_info.value.operation == "always_fails"

    def test_cause_is_preserved_in_max_retries_error(self) -> None:
        original = ConnectionError("original error")

        @network_retry
        def always_fails() -> None:
            raise original

        with pytest.raises(MaxRetriesExceededError) as exc_info:
            always_fails()

        assert exc_info.value.cause is original

    def test_does_not_retry_on_non_retryable_error(self) -> None:
        call_count = 0

        @network_retry
        def raises_value_error() -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            raises_value_error()

        assert call_count == 1

    def test_succeeds_on_second_attempt(self) -> None:
        call_count = 0

        @network_retry
        def flaky() -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TimeoutError("timeout")
            return call_count

        result = flaky()
        assert result == 2
        assert call_count == 2


class TestStorageRetry:
    def test_succeeds_on_first_attempt(self) -> None:
        @storage_retry
        def write() -> str:
            return "written"

        assert write() == "written"

    def test_retries_on_os_error(self) -> None:
        call_count = 0

        @storage_retry
        def write() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise OSError("disk full")
            return "ok"

        assert write() == "ok"
        assert call_count == 2

    def test_raises_max_retries_after_all_attempts_fail(self) -> None:
        @storage_retry
        def write() -> None:
            raise OSError("disk full")

        with pytest.raises(MaxRetriesExceededError):
            write()

    def test_does_not_retry_duplicate_record_error(self) -> None:
        from economics_pipeline.exceptions.storage import DuplicateRecordError

        call_count = 0

        @storage_retry
        def write() -> None:
            nonlocal call_count
            call_count += 1
            raise DuplicateRecordError("key")

        with pytest.raises(DuplicateRecordError):
            write()

        assert call_count == 1
