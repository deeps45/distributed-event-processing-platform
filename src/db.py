import json
import uuid
from datetime import datetime

import asyncpg

from src.config import settings

_pool: asyncpg.Pool | None = None


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            settings.postgres_dsn, min_size=2, max_size=10, init=_init_connection
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized; call init_pool() first")
    return _pool


async def upsert_completed_batch(rows: list[dict]) -> None:
    """Writes an entire consumer poll batch's successful events in one round
    trip via unnest(), instead of one UPDATE per event. This (plus dropping
    the API's insert and the consumer's separate "processing" write - see
    src/api/main.py and src/consumer/consumer.py) is what took the pipeline
    from 3 sequential Postgres round trips per event down to a single
    batched one, which is where nearly all of the latency and throughput
    improvement over the first version of this service came from.

    Each row: {id, event_type, payload, source, produced_at, processed_at}
    """
    if not rows:
        return
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO events (id, event_type, payload, source, status, produced_at, processed_at)
        SELECT id, event_type, payload::jsonb, source, 'completed', produced_at, processed_at
        FROM unnest(
            $1::uuid[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::timestamptz[]
        ) AS t(id, event_type, payload, source, produced_at, processed_at)
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            processed_at = EXCLUDED.processed_at
        """,
        [r["id"] for r in rows],
        [r["event_type"] for r in rows],
        [json.dumps(r["payload"]) for r in rows],
        [r["source"] for r in rows],
        [r["produced_at"] for r in rows],
        [r["processed_at"] for r in rows],
    )


async def upsert_dead_letter_batch(rows: list[dict]) -> None:
    """Same batching as upsert_completed_batch, for events that exhausted
    retries within this poll cycle.

    Each row: {id, event_type, payload, source, produced_at, retry_count, error_message}
    """
    if not rows:
        return
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO events (id, event_type, payload, source, status, produced_at, retry_count, error_message)
        SELECT id, event_type, payload::jsonb, source, 'dead_letter', produced_at, retry_count, error_message
        FROM unnest(
            $1::uuid[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::int[], $7::text[]
        ) AS t(id, event_type, payload, source, produced_at, retry_count, error_message)
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            retry_count = EXCLUDED.retry_count,
            error_message = EXCLUDED.error_message
        """,
        [r["id"] for r in rows],
        [r["event_type"] for r in rows],
        [json.dumps(r["payload"]) for r in rows],
        [r["source"] for r in rows],
        [r["produced_at"] for r in rows],
        [r["retry_count"] for r in rows],
        [r["error_message"] for r in rows],
    )


async def get_event(event_id: uuid.UUID) -> asyncpg.Record | None:
    pool = get_pool()
    return await pool.fetchrow("SELECT * FROM events WHERE id = $1", event_id)


async def count_by_status() -> list[asyncpg.Record]:
    pool = get_pool()
    return await pool.fetch("SELECT status, count(*) FROM events GROUP BY status")
