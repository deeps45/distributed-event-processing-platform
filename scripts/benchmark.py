"""Producer-level A/B benchmark: naive synchronous send-one-at-a-time vs the
platform's actual async, batched (linger_ms), concurrent producer.

This isolates the exact design decision the README's latency claim rests on,
independent of HTTP client overhead or consumer processing time. Numbers
below are measured every run, not hardcoded - see README for a specific run's
output.

Requires a running Kafka broker (`docker compose up -d kafka`) and the repo's
dependencies installed locally (`pip install -r requirements.txt`).
"""

import asyncio
import statistics
import time

from aiokafka import AIOKafkaProducer

from src.config import settings
from src.schemas import Event

N_EVENTS = 2000
CONCURRENCY = 100


def make_event(i: int) -> Event:
    return Event(event_type="benchmark.load", payload={"i": i}, source="benchmark")


async def run_sync_baseline(n: int) -> list[float]:
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, acks="all")
    await producer.start()
    latencies: list[float] = []
    try:
        for i in range(n):
            event = make_event(i)
            start = time.perf_counter()
            await producer.send_and_wait(
                settings.kafka_topic_events, key=str(event.id).encode(), value=event.to_kafka_json()
            )
            latencies.append(time.perf_counter() - start)
    finally:
        await producer.stop()
    return latencies


async def run_async_pipeline(n: int, concurrency: int) -> list[float]:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        linger_ms=10,
        compression_type="gzip",
    )
    await producer.start()
    latencies: list[float] = []
    semaphore = asyncio.Semaphore(concurrency)

    async def send_one(i: int) -> None:
        event = make_event(i)
        start = time.perf_counter()
        async with semaphore:
            await producer.send_and_wait(
                settings.kafka_topic_events, key=str(event.id).encode(), value=event.to_kafka_json()
            )
        latencies.append(time.perf_counter() - start)

    try:
        await asyncio.gather(*(send_one(i) for i in range(n)))
    finally:
        await producer.stop()
    return latencies


def summarize(name: str, latencies: list[float], wall_seconds: float) -> dict:
    avg_ms = statistics.mean(latencies) * 1000
    p95_ms = statistics.quantiles(latencies, n=20)[18] * 1000
    throughput = len(latencies) / wall_seconds
    return {
        "name": name,
        "count": len(latencies),
        "avg_latency_ms": round(avg_ms, 2),
        "p95_latency_ms": round(p95_ms, 2),
        "wall_seconds": round(wall_seconds, 2),
        "throughput_events_per_sec": round(throughput, 1),
        "extrapolated_events_per_day": round(throughput * 86400),
    }


async def main() -> None:
    print(f"Running sync baseline: {N_EVENTS} events, one send_and_wait() at a time...")
    start = time.perf_counter()
    sync_latencies = await run_sync_baseline(N_EVENTS)
    sync_stats = summarize("sync_baseline", sync_latencies, time.perf_counter() - start)

    print(f"Running async pipeline: {N_EVENTS} events, concurrency={CONCURRENCY}, linger_ms=10...")
    start = time.perf_counter()
    async_latencies = await run_async_pipeline(N_EVENTS, CONCURRENCY)
    async_stats = summarize("async_pipeline", async_latencies, time.perf_counter() - start)

    reduction_pct = (
        (sync_stats["avg_latency_ms"] - async_stats["avg_latency_ms"]) / sync_stats["avg_latency_ms"] * 100
    )

    print("\n=== Results ===")
    print(sync_stats)
    print(async_stats)
    print(f"\nAverage per-event latency reduction (async vs sync): {reduction_pct:.1f}%")
    print(
        f"Sustained throughput (async pipeline): {async_stats['throughput_events_per_sec']} events/sec "
        f"(~{async_stats['extrapolated_events_per_day']:,} events/day)"
    )


if __name__ == "__main__":
    asyncio.run(main())
