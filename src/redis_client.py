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
    """Atomically claims an event id for processing.

    Returns True if this is the first time we've seen the id (caller should
    process it), False if it was already processed (caller should skip it).
    This is what makes the at-least-once Kafka delivery behave as
    effectively-once from the consumer's point of view.
    """
    client = get_redis()
    was_set = await client.set(
        f"event:processed:{event_id}", "1", nx=True, ex=settings.idempotency_ttl_seconds
    )
    return bool(was_set)
