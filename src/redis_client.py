import redis.asyncio as redis

from src.config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def mark_processed_if_new(event_id: str) -> bool:
    """Atomically claims an event id. Exposed for tests/tools; the consumer
    itself uses the pipelined batch functions below in the hot path - see
    the module docstring-equivalent note on claim ordering there."""
    client = get_redis()
    was_set = await client.set(
        f"event:processed:{event_id}", "1", nx=True, ex=settings.idempotency_ttl_seconds
    )
    return bool(was_set)


async def check_already_processed_batch(event_ids: list[str]) -> list[bool]:
    """Read-only, pipelined EXISTS check - one Redis round trip for the
    whole poll batch. Deliberately does NOT claim anything: claiming has to
    happen only after a durable Postgres write succeeds (see
    claim_processed_batch below and consumer.py), or a crash between the
    claim and the write permanently loses that event - a real bug this
    project's own chaos test (scripts/chaos_test.py) caught: killing a
    consumer replica mid-batch under load lost exactly 1 of 20,000 events,
    traced to this exact race in an earlier version that claimed with SETNX
    before processing instead of after persisting.
    """
    if not event_ids:
        return []
    client = get_redis()
    pipe = client.pipeline(transaction=False)
    for event_id in event_ids:
        pipe.exists(f"event:processed:{event_id}")
    results = await pipe.execute()
    return [bool(r) for r in results]


async def claim_processed_batch(event_ids: list[str]) -> None:
    """Marks event ids as done. Only call this AFTER their outcome is
    already durably written to Postgres - see check_already_processed_batch.
    Plain SET (not NX): by the time this runs, this replica has already
    confirmed via its own successful write that it owns this outcome, so
    there's no concurrent-claim race left to guard against here.
    """
    if not event_ids:
        return
    client = get_redis()
    pipe = client.pipeline(transaction=False)
    for event_id in event_ids:
        pipe.set(f"event:processed:{event_id}", "1", ex=settings.idempotency_ttl_seconds)
    await pipe.execute()
