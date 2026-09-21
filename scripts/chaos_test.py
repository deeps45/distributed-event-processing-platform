"""Chaos test: kills a live consumer replica mid-load and proves the
platform recovers with zero data loss, instead of just asserting that it
would.

Most "fault-tolerant" claims in a README are backed by code that's never
actually been made to fail. This script does the opposite: it starts a
sustained batch of events, hard-kills (SIGKILL, not a graceful stop) one
running consumer replica partway through - the worst case, an abrupt crash
with whatever it was holding mid-batch abandoned - and then verifies three
things independently:

  1. The killed replica's partitions were reassigned to survivors (via
     `kafka-consumer-groups.sh --describe`).
  2. Every single event in the batch eventually lands in Postgres - the
     killed replica's in-flight work isn't lost, because offsets are only
     committed after a batch is durably written (see consumer.py), so
     whatever it was mid-processing gets redelivered to whoever picks up
     that partition next.
  3. How long the whole thing took, end to end.

Requires the full stack running with multiple consumer replicas, e.g.:
  docker compose up -d --build --scale consumer=3
"""

import argparse
import asyncio
import subprocess
import time
import uuid

import asyncpg
from aiokafka import AIOKafkaProducer

from src.config import settings
from src.schemas import Event

EVENT_TYPE = "chaos.test"
COMPOSE_SERVICE = "consumer"


def sh(*args: str) -> str:
    return subprocess.run(args, capture_output=True, text=True, check=True).stdout.strip()


def pick_a_consumer_container() -> str:
    ids = sh("docker", "compose", "ps", "-q", COMPOSE_SERVICE).splitlines()
    if len(ids) < 2:
        raise SystemExit(
            f"Only {len(ids)} '{COMPOSE_SERVICE}' replica(s) running. Need at least 2 so killing one "
            f"still leaves survivors to take over its partitions - try:\n"
            f"  docker compose up -d --build --scale consumer=3"
        )
    return ids[0]


async def produce(n: int, run_tag: str, kill_at: int, container_id: str) -> float:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        linger_ms=10,
        compression_type="gzip",
    )
    await producer.start()
    start = time.perf_counter()
    killed = False
    try:
        for i in range(n):
            event = Event(event_type=EVENT_TYPE, payload={"run": run_tag, "i": i}, source="chaos-test")
            await producer.send_and_wait(
                settings.kafka_topic_events, key=str(event.id).encode(), value=event.to_kafka_json()
            )
            if not killed and i >= kill_at:
                print(f"  [{i}/{n}] SIGKILL-ing consumer container {container_id[:12]} now...")
                sh("docker", "kill", "-s", "SIGKILL", container_id)
                killed = True
    finally:
        await producer.stop()
    return time.perf_counter() - start


def describe_group() -> list[list[str]]:
    out = sh(
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--group",
        settings.kafka_consumer_group,
    )
    return [l.split() for l in out.splitlines() if l.strip() and not l.startswith("GROUP")]


def wait_for_rebalance(expected_survivor_count: int, timeout_seconds: float = 60) -> float:
    """Polls until the group has settled on exactly `expected_survivor_count`
    live members with every partition holding a real offset (not "-", which
    Kafka shows mid-rebalance) - not just "no dashes right now", which could
    be a false positive caught before the coordinator has even noticed the
    kill (session timeout hasn't elapsed yet). Requiring the member count to
    have actually dropped is what proves a rebalance really happened.
    """
    start = time.perf_counter()
    while time.perf_counter() - start < timeout_seconds:
        rows = describe_group()
        offsets = {r[3] for r in rows if len(r) > 3}
        consumer_ids = {r[6] for r in rows if len(r) > 6}
        if rows and "-" not in offsets and len(consumer_ids) == expected_survivor_count:
            return time.perf_counter() - start
        time.sleep(1)
    raise TimeoutError(
        f"consumer group did not settle on {expected_survivor_count} live members within the timeout"
    )


async def wait_for_drain(pool: asyncpg.Pool, run_tag: str, expected: int, timeout_seconds: float = 120) -> None:
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
    raise TimeoutError(
        f"only {count}/{expected} events were recovered after the kill - this would be a real data-loss bug"
    )


async def main(n: int, kill_at: int) -> None:
    run_tag = uuid.uuid4().hex[:8]
    container_id = pick_a_consumer_container()
    replicas_before = len(sh("docker", "compose", "ps", "-q", COMPOSE_SERVICE).splitlines())

    pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=2)

    print(f"Starting chaos test: {n} events, killing one of {replicas_before} consumer replicas at event #{kill_at}")
    produce_seconds = await produce(n, run_tag, kill_at, container_id)
    print(f"Finished producing {n} events in {produce_seconds:.2f}s")

    print("Waiting for Kafka to reassign the dead replica's partitions...")
    rebalance_seconds = wait_for_rebalance(expected_survivor_count=replicas_before - 1)
    print(f"Rebalance complete in {rebalance_seconds:.1f}s")

    print("Waiting for every event to land in Postgres (including whatever the killed replica had in flight)...")
    drain_start = time.perf_counter()
    await wait_for_drain(pool, run_tag, n)
    drain_seconds = time.perf_counter() - drain_start

    row = await pool.fetchrow(
        "SELECT count(*) AS n, count(*) FILTER (WHERE status = 'dead_letter') AS dead_lettered "
        "FROM events WHERE event_type = $1 AND payload->>'run' = $2",
        EVENT_TYPE,
        run_tag,
    )

    lost = n - row["n"]
    loss_summary = "NONE" if lost == 0 else f"{lost} EVENTS LOST"

    print("\n=== Chaos test result ===")
    print(f"Events sent: {n}")
    print(f"Events recovered in Postgres: {row['n']} (dead-lettered: {row['dead_lettered']})")
    print(f"Data loss: {loss_summary}")
    print(f"Time to reassign dead replica's partitions: {rebalance_seconds:.1f}s")
    print(f"Time to fully drain after the kill: {drain_seconds:.1f}s")
    print(
        f"\nNote: the killed container is still dead - restore replica count with:\n"
        f"  docker compose up -d --build --scale {COMPOSE_SERVICE}={replicas_before}"
    )

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--total", type=int, default=20000)
    parser.add_argument("--kill-at", type=int, default=5000, help="event index at which to SIGKILL a replica")
    args = parser.parse_args()
    asyncio.run(main(args.total, args.kill_at))
