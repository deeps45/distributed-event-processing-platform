import asyncio
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server

from src import db
from src.common.logging_config import configure_logging
from src.common.retry import retry_with_backoff
from src.config import settings
from src.consumer.processor import process_event
from src.monitoring.metrics import (
    EVENTS_DEAD_LETTERED,
    EVENTS_DUPLICATE,
    EVENTS_FAILED,
    EVENTS_PROCESSED,
    IN_FLIGHT_EVENTS,
    PROCESSING_LATENCY_SECONDS,
)
from src.producer.producer import EventProducer
from src.redis_client import get_redis
from src.schemas import Event

configure_logging("consumer")
logger = logging.getLogger(__name__)

# Bounds how many events this consumer process handles concurrently. Kept
# well under aiokafka's default max_poll_interval_ms so a slow batch never
# trips a group rebalance.
CONCURRENCY = 20


async def check_idempotency_batch(events: list[Event]) -> list[bool]:
    """One Redis round trip for the whole poll batch instead of one per
    event: still an atomic SET NX per key (each command is independently
    atomic), just pipelined so N events cost 1 network round trip, not N.
    """
    client = get_redis()
    pipe = client.pipeline(transaction=False)
    for event in events:
        pipe.set(f"event:processed:{event.id}", "1", nx=True, ex=settings.idempotency_ttl_seconds)
    results = await pipe.execute()
    return [bool(r) for r in results]


async def handle_message(
    event: Event, is_new: bool, dlq_producer: EventProducer, semaphore: asyncio.Semaphore
) -> dict | None:
    """Processes one event and returns an outcome row for the batched DB
    write the caller issues once per poll cycle - this function itself does
    no DB I/O, which is what makes that batching possible.
    """
    async with semaphore:
        IN_FLIGHT_EVENTS.inc()
        try:
            if not is_new:
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

    try:
        while True:
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
                await consumer.commit()
                continue

            is_new_flags = await check_idempotency_batch(events)
            outcomes = await asyncio.gather(
                *(
                    handle_message(event, is_new, dlq_producer, semaphore)
                    for event, is_new in zip(events, is_new_flags)
                )
            )

            completed_rows = [o for o in outcomes if o and o["outcome"] == "completed"]
            dead_letter_rows = [o for o in outcomes if o and o["outcome"] == "dead_letter"]

            # Two DB round trips total for this whole batch (of up to 500
            # events), not one per event - see db.py for why.
            await db.upsert_completed_batch(completed_rows)
            await db.upsert_dead_letter_batch(dead_letter_rows)

            # Committed once per fetched batch, after every message in it has
            # either completed or been routed to the DLQ, so a crash never
            # loses a message outright: at-least-once delivery, made
            # effectively-once by the Redis idempotency check above.
            await consumer.commit()
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        await db.close_pool()


if __name__ == "__main__":
    asyncio.run(run())
