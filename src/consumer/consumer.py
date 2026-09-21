import asyncio
import logging
import signal
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from prometheus_client import start_http_server

from src import db
from src.common.logging_config import configure_logging
from src.common.retry import CircuitBreaker, retry_with_backoff
from src.config import settings
from src.consumer.processor import process_event
from src.monitoring.metrics import (
    DB_WRITE_FAILURES,
    EVENTS_DEAD_LETTERED,
    EVENTS_DUPLICATE,
    EVENTS_FAILED,
    EVENTS_PROCESSED,
    IN_FLIGHT_EVENTS,
    PROCESSING_LATENCY_SECONDS,
)
from src.producer.producer import EventProducer
from src.redis_client import check_already_processed_batch, claim_processed_batch, close_redis
from src.schemas import Event

configure_logging("consumer")
logger = logging.getLogger(__name__)

# Bounds how many events this consumer process handles concurrently. Kept
# well under aiokafka's default max_poll_interval_ms so a slow batch never
# trips a group rebalance.
CONCURRENCY = 20

# Guards the batched Postgres write. Opens after 3 consecutive failures and
# fails fast for 10s rather than let every poll cycle hang on a dead
# connection pool - see flush_to_postgres().
_db_breaker = CircuitBreaker(failure_threshold=3, reset_timeout_seconds=10.0)


async def flush_to_postgres(completed_rows: list[dict], dead_letter_rows: list[dict]) -> None:
    """Persists one poll cycle's outcomes, retrying indefinitely (through
    the circuit breaker) rather than giving up after N attempts.

    This is a deliberate asymmetry with retry_with_backoff elsewhere:
    event *processing* retries are bounded because giving up routes to the
    DLQ, a well-defined outcome. There's no equivalent fallback for a
    failed Postgres write - dropping it would silently lose an event's
    result - so this applies backpressure to this consumer replica
    instead (it stops pulling new batches; Kafka offsets for this batch
    stay uncommitted) until the write succeeds. A crash mid-outage just
    means Kafka redelivers the batch once a replica is healthy again.
    """

    async def _write() -> None:
        await db.upsert_completed_batch(completed_rows)
        await db.upsert_dead_letter_batch(dead_letter_rows)

    attempt = 0
    while True:
        attempt += 1
        try:
            await _db_breaker.call(_write)
            return
        except Exception as exc:
            DB_WRITE_FAILURES.inc()
            wait_seconds = min(10.0, 0.5 * attempt)
            logger.error(
                "failed to persist batch to Postgres; applying backpressure and retrying",
                extra={"attempt": attempt, "error": str(exc), "retry_in_seconds": wait_seconds},
            )
            await asyncio.sleep(wait_seconds)


async def commit_tolerantly(consumer: AIOKafkaConsumer) -> None:
    """See the inline comment at the call site in run() for why a failed
    commit here is logged and swallowed rather than raised."""
    try:
        await consumer.commit()
    except KafkaError as exc:
        logger.warning(
            "offset commit failed, likely due to a concurrent rebalance; "
            "continuing (any batch written this cycle is already durable)",
            extra={"error": str(exc)},
        )


async def handle_message(
    event: Event, already_processed: bool, dlq_producer: EventProducer, semaphore: asyncio.Semaphore
) -> dict | None:
    """Processes one event and returns an outcome row for the batched DB
    write the caller issues once per poll cycle - this function itself does
    no DB I/O, which is what makes that batching possible.
    """
    async with semaphore:
        IN_FLIGHT_EVENTS.inc()
        try:
            if already_processed:
                EVENTS_DUPLICATE.labels(event_type=event.event_type).inc()
                return None

            try:
                await retry_with_backoff(
                    lambda: process_event(event),
                    max_attempts=settings.max_retries,
                    base_seconds=settings.retry_backoff_base_seconds,
                )
            except Exception as exc:
                EVENTS_FAILED.labels(event_type=event.event_type).inc()
                await dlq_producer.send_event(event, topic=settings.kafka_topic_dlq)
                EVENTS_DEAD_LETTERED.labels(event_type=event.event_type).inc()
                logger.error(
                    "event dead-lettered after exhausting retries",
                    extra={"event_id": str(event.id), "error": str(exc)},
                )
                return {
                    "outcome": "dead_letter",
                    "id": event.id,
                    "event_type": event.event_type,
                    "payload": event.payload,
                    "source": event.source,
                    "produced_at": event.produced_at,
                    "retry_count": settings.max_retries,
                    "error_message": str(exc),
                }

            processed_at = datetime.now(timezone.utc)
            latency = (processed_at - event.produced_at).total_seconds()
            PROCESSING_LATENCY_SECONDS.observe(latency)
            EVENTS_PROCESSED.labels(event_type=event.event_type).inc()
            return {
                "outcome": "completed",
                "id": event.id,
                "event_type": event.event_type,
                "payload": event.payload,
                "source": event.source,
                "produced_at": event.produced_at,
                "processed_at": processed_at,
            }
        finally:
            IN_FLIGHT_EVENTS.dec()


async def run() -> None:
    # Each consumer replica exposes its own Prometheus endpoint on the
    # container network (not published to the host) so `docker compose up
    # --scale consumer=N` never collides on a host port.
    start_http_server(9100)

    await db.init_pool()

    dlq_producer = EventProducer()
    await dlq_producer.start()

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_events,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("consumer started", extra={"group": settings.kafka_consumer_group})

    semaphore = asyncio.Semaphore(CONCURRENCY)

    # SIGTERM is what `docker stop` / a Kubernetes pod eviction / `docker
    # compose down` send. Without this, a replica dies mid-batch: whatever
    # it was holding in the semaphore is abandoned (fine, Kafka redelivers
    # it - see the commit comment below) but it's a harder stop than
    # necessary. This lets a signaled replica finish its current batch,
    # commit, and exit cleanly instead.
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        while not stop_event.is_set():
            batches = await consumer.getmany(timeout_ms=1000, max_records=500)
            if not batches:
                continue

            events: list[Event] = []
            for _tp, messages in batches.items():
                for msg in messages:
                    try:
                        events.append(Event.from_kafka_json(msg.value))
                    except Exception:
                        logger.exception("failed to parse message, skipping", extra={"offset": msg.offset})

            if not events:
                await commit_tolerantly(consumer)
                continue

            already_processed_flags = await check_already_processed_batch([str(e.id) for e in events])
            outcomes = await asyncio.gather(
                *(
                    handle_message(event, already_processed, dlq_producer, semaphore)
                    for event, already_processed in zip(events, already_processed_flags)
                )
            )

            completed_rows = [o for o in outcomes if o and o["outcome"] == "completed"]
            dead_letter_rows = [o for o in outcomes if o and o["outcome"] == "dead_letter"]

            # Two DB round trips total for this whole batch (of up to 500
            # events), not one per event - see db.py for why. Blocks (with
            # backpressure) until the write actually succeeds - see
            # flush_to_postgres().
            await flush_to_postgres(completed_rows, dead_letter_rows)

            # Idempotency is claimed only now, strictly after the batch is
            # durably in Postgres - never before processing. Claiming
            # earlier (the original version of this consumer) creates a
            # window where a crash between the claim and the write
            # permanently loses the event: the claim survives the crash,
            # so a redelivery of the same message is skipped as a "already
            # done" duplicate that was, in fact, never persisted. Found via
            # scripts/chaos_test.py (1 lost event out of 20,000 on a killed
            # replica) - see redis_client.py.
            await claim_processed_batch([str(r["id"]) for r in completed_rows + dead_letter_rows])

            # Committed once per fetched batch, after every message in it has
            # either completed or been routed to the DLQ, so a crash never
            # loses a message outright: at-least-once delivery, made
            # effectively-once by the Redis idempotency check above.
            #
            # commit_tolerantly (not a raw consumer.commit()) because a
            # sibling replica dying triggers a group-wide rebalance for
            # every member, not just a reassignment of the dead one's
            # partitions - if this replica's assignment changed between
            # fetching this batch and committing it, the commit is for a
            # now-stale generation and Kafka correctly rejects it
            # (CommitFailedError). Left unhandled, this crashes an
            # otherwise perfectly healthy replica - found via
            # scripts/chaos_test.py, where killing exactly one replica
            # took down a second, unrelated one this way. It's safe to
            # just log and move on: this batch's outcomes are already
            # durably in Postgres and already claimed in Redis (both
            # happened above, before this commit), so whoever now owns
            # these partitions will either see correctly-committed offsets
            # already, or redeliver a batch this replica will recognize as
            # already-processed and skip - no data loss, no
            # double-processing either way.
            await commit_tolerantly(consumer)

        logger.info("stop signal received and current batch drained; shutting down cleanly")
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        await close_redis()
        await db.close_pool()


if __name__ == "__main__":
    asyncio.run(run())
