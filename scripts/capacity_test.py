"""Measures the consumer pipeline's own sustained throughput, decoupled from
any load-testing client's limits.

scripts/generate_load.py drives traffic through the REST API using a single
Python process's httpx.AsyncClient, which has its own ceiling (observed
~170 events/sec) - Kafka consumer lag stayed near zero the whole time that
was running, meaning the consumers were waiting on the HTTP client, not the
other way around. This script instead publishes directly to Kafka (the same
path the API's producer takes, minus the HTTP hop) and measures how fast the
consumer group + batched Postgres writes can actually drain a batch, using
Postgres's own processed_at timestamps rather than wall-clock guesses.

Requires the full stack running (`docker compose up -d`).
"""

import argparse
import asyncio
import time
import uuid

import asyncpg
from aiokafka import AIOKafkaProducer

from src.config import settings
from src.schemas import Event

EVENT_TYPE = "capacity.test"


async def produce(n: int, concurrency: int, run_tag: str) -> float:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        linger_ms=10,
        compression_type="gzip",
    )
    await producer.start()
    semaphore = asyncio.Semaphore(concurrency)

    async def send_one(i: int) -> None:
        event = Event(event_type=EVENT_TYPE, payload={"run": run_tag, "i": i}, source="capacity-test")
        async with semaphore:
            await producer.send_and_wait(
                settings.kafka_topic_events, key=str(event.id).encode(), value=event.to_kafka_json()
            )

    start = time.perf_counter()
    try:
        await asyncio.gather(*(send_one(i) for i in range(n)))
    finally:
        await producer.stop()
    return time.perf_counter() - start


async def wait_for_drain(pool: asyncpg.Pool, run_tag: str, expected: int, timeout_seconds: float = 600) -> None:
    deadline = time.perf_counter() + timeout_seconds
    while time.perf_counter() < deadline:
        count = await pool.fetchval(
            "SELECT count(*) FROM events WHERE event_type = $1 AND payload->>'run' = $2",
            EVENT_TYPE,
            run_tag,
        )
        print(f"  drained so far: {count}/{expected}")
        if count >= expected:
            return
        await asyncio.sleep(1.0)
    raise TimeoutError("consumer group did not finish draining the batch within the timeout")


async def main(n: int, concurrency: int) -> None:
    run_tag = uuid.uuid4().hex[:8]
    pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=2)

    print(f"Producing {n} events directly to Kafka (bypassing the API), run={run_tag}...")
    produce_seconds = await produce(n, concurrency, run_tag)
    print(f"Produced {n} events in {produce_seconds:.2f}s ({n / produce_seconds:.1f} events/sec)")

    print("Waiting for the consumer group to fully drain this batch into Postgres...")
    await wait_for_drain(pool, run_tag, n)

    row = await pool.fetchrow(
        """
        SELECT min(processed_at) AS first, max(processed_at) AS last, count(*) AS n
        FROM events WHERE event_type = $1 AND payload->>'run' = $2
        """,
        EVENT_TYPE,
        run_tag,
    )
    elapsed = (row["last"] - row["first"]).total_seconds() or 0.001
    throughput = row["n"] / elapsed

    print("\n=== Consumer pipeline sustained throughput ===")
    print(f"Events consumed: {row['n']}")
    print(f"Consumption window (first to last processed_at): {elapsed:.2f}s")
    print(f"Throughput: {throughput:.1f} events/sec")
    print(f"Extrapolated: {throughput * 86400:,.0f} events/day")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int, default=50000)
    parser.add_argument("--concurrency", type=int, default=200)
    args = parser.parse_args()
    asyncio.run(main(args.total, args.concurrency))
