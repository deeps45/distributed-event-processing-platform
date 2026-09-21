"""Drives realistic traffic through the public API (producer -> Kafka ->
consumer -> Postgres), useful for a manual smoke test or for watching the
DLQ/retry path trigger under SIMULATE_FAILURE_RATE.

For the actual latency/throughput benchmark used in the README, see
scripts/benchmark.py, which measures the producer directly to isolate it
from HTTP client overhead.
"""

import argparse
import asyncio
import random
import time

import httpx

EVENT_TYPES = ["order.created", "payment.processed", "user.signup", "inventory.updated"]


async def send_event(client: httpx.AsyncClient, api_url: str, i: int) -> None:
    payload = {
        "event_type": random.choice(EVENT_TYPES),
        "payload": {"index": i, "amount": round(random.uniform(1, 500), 2)},
        "source": "load-generator",
    }
    resp = await client.post(f"{api_url}/events", json=payload, timeout=10)
    resp.raise_for_status()


async def main(api_url: str, total: int, concurrency: int) -> None:
    semaphore = asyncio.Semaphore(concurrency)

    async def bound_send(client: httpx.AsyncClient, i: int) -> None:
        async with semaphore:
            await send_event(client, api_url, i)

    start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*(bound_send(client, i) for i in range(total)))
    elapsed = time.perf_counter() - start

    print(f"Sent {total} events in {elapsed:.2f}s ({total / elapsed:.1f} events/sec)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--total", type=int, default=5000)
    parser.add_argument("--concurrency", type=int, default=100)
    args = parser.parse_args()

    asyncio.run(main(args.api_url, args.total, args.concurrency))
