import pytest

from src.common.retry import CircuitBreaker, CircuitBreakerOpen, retry_with_backoff


@pytest.mark.asyncio
async def test_retry_with_backoff_succeeds_eventually():
    attempts = 0

    async def flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("not yet")
        return "ok"

    result = await retry_with_backoff(flaky, max_attempts=5, base_seconds=0.001)
    assert result == "ok"
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_with_backoff_raises_after_max_attempts():
    async def always_fails():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        await retry_with_backoff(always_fails, max_attempts=3, base_seconds=0.001)


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_threshold():
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout_seconds=60)

    async def fails():
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(fails)

    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(fails)


@pytest.mark.asyncio
async def test_circuit_breaker_recovers_after_success():
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=0)

    async def fails():
        raise RuntimeError("boom")

    async def succeeds():
        return "ok"

    with pytest.raises(RuntimeError):
        await breaker.call(fails)

    # reset_timeout_seconds=0 means the breaker is immediately half-open
    result = await breaker.call(succeeds)
    assert result == "ok"
