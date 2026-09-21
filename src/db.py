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


async def insert_pending_event(event_id: uuid.UUID, event_type: str, payload: dict, source: str, produced_at: datetime) -> None:
    pool = get_pool()
    await pool.execute(
        """
        INSERT INTO events (id, event_type, payload, source, status, produced_at)
        VALUES ($1, $2, $3, $4, 'pending', $5)
        ON CONFLICT (id) DO NOTHING
        """,
        event_id,
        event_type,
        payload,
        source,
        produced_at,
    )


async def mark_processing(event_id: uuid.UUID) -> None:
    pool = get_pool()
    await pool.execute("UPDATE events SET status = 'processing' WHERE id = $1", event_id)


async def mark_completed(event_id: uuid.UUID, processed_at: datetime) -> None:
    pool = get_pool()
    await pool.execute(
        "UPDATE events SET status = 'completed', processed_at = $2 WHERE id = $1",
        event_id,
        processed_at,
    )


async def mark_retrying(event_id: uuid.UUID, retry_count: int, error_message: str) -> None:
    pool = get_pool()
    await pool.execute(
        "UPDATE events SET status = 'retrying', retry_count = $2, error_message = $3 WHERE id = $1",
        event_id,
        retry_count,
        error_message,
    )


async def mark_dead_letter(event_id: uuid.UUID, retry_count: int, error_message: str) -> None:
    pool = get_pool()
    await pool.execute(
        "UPDATE events SET status = 'dead_letter', retry_count = $2, error_message = $3 WHERE id = $1",
        event_id,
        retry_count,
        error_message,
    )


async def get_event(event_id: uuid.UUID) -> asyncpg.Record | None:
    pool = get_pool()
    return await pool.fetchrow("SELECT * FROM events WHERE id = $1", event_id)


async def count_by_status() -> list[asyncpg.Record]:
    pool = get_pool()
    return await pool.fetch("SELECT status, count(*) FROM events GROUP BY status")
