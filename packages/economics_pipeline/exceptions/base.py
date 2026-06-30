"""Root exception hierarchy for the economics pipeline."""

from __future__ import annotations


class PipelineError(Exception):
    """
    Base class for all pipeline exceptions.

    Catching PipelineError in a service gives you a single
    catch-all for anything the pipeline intentionally raises,
    while still letting unexpected bugs propagate as plain Exception.
    """

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base} (caused by: {self.cause})" if self.cause else base


class MaxRetriesExceededError(PipelineError):
    """
    Raised when a retryable operation fails every attempt.

    Wraps the tenacity RetryError so callers don't need to import tenacity.
    """

    def __init__(self, operation: str, attempts: int, cause: BaseException | None = None) -> None:
        self.operation = operation
        self.attempts = attempts
        super().__init__(
            f"'{operation}' failed after {attempts} attempt(s)",
            cause=cause,
        )
