import asyncio
import logging
import time
from typing import Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def retry_with_backoff(
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: int,
    base_seconds: float,
    max_seconds: float = 30.0,
) -> T:
    """Retries an async callable with full-jitter exponential backoff.

    Raises the last exception once max_attempts is exhausted so the caller
    can decide what happens next (e.g. route to a dead-letter queue).
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fn()
        except Exception:
            if attempt >= max_attempts:
                raise
            delay = min(max_seconds, base_seconds * (2 ** (attempt - 1)))
            jitter = delay * 0.5 * (1 - (2 * (time.time() % 1)))
            sleep_for = max(0.0, delay + jitter) / 2
            logger.warning(
                "retrying after failure",
                extra={"attempt": attempt, "max_attempts": max_attempts, "sleep_seconds": sleep_for},
            )
            await asyncio.sleep(sleep_for)


class CircuitBreakerOpen(Exception):
    pass


class CircuitBreaker:
    """Minimal circuit breaker guarding a downstream dependency (DB/Redis/etc.).

    Opens after `failure_threshold` consecutive failures, refuses calls for
    `reset_timeout_seconds`, then allows a single trial call (half-open)
    before fully closing again on success.
    """

    def __init__(self, failure_threshold: int = 5, reset_timeout_seconds: float = 15.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout_seconds = reset_timeout_seconds
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    def _state(self) -> str:
        if self._opened_at is None:
            return "closed"
        if time.time() - self._opened_at >= self.reset_timeout_seconds:
            return "half_open"
        return "open"

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        state = self._state()
        if state == "open":
            raise CircuitBreakerOpen("circuit breaker is open; refusing call")

        try:
            result = await fn()
        except Exception:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                self._opened_at = time.time()
                logger.error("circuit breaker opened", extra={"failures": self._consecutive_failures})
            raise
        else:
            self._consecutive_failures = 0
            self._opened_at = None
            return result
