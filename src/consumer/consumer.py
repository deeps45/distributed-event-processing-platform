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
from src.redis_client import mark_processed_if_new
from src.schemas import Event

configure_logging("consumer")
logger = logging.getLogger(__name__)

# Bounds how many events this consumer process handles concurrently. Kept
# well under aiokafka's default max_poll_interval_ms so a slow batch never
# trips a group rebalance.
CONCURRENCY = 20


async def handle_message(event: Event, dlq_producer: EventProducer, semaphore: asyncio.Semaphore) -> None:
    async with semaphore:
        IN_FLIGHT_EVENTS.inc()
        try:
            is_new = await mark_processed_if_new(str(event.id))
            if not is_new:
                EVENTS_DUPLICATE.labels(event_type=event.event_type).inc()
                return

            await db.mark_processing(event.id)

            try:
                await retry_with_backoff(
                    lambda: process_event(event),
                    max_attempts=settings.max_retries,
                    base_seconds=settings.retry_backoff_base_seconds,
                )
            except Exception as exc:
                EVENTS_FAILED.labels(event_type=event.event_type).inc()
                await db.mark_dead_letter(event.id, settings.max_retries, str(exc))
                await dlq_producer.send_event(event, topic=settings.kafka_topic_dlq)
                EVENTS_DEAD_LETTERED.labels(event_type=event.event_type).inc()
                logger.error(
                    "event dead-lettered after exhausting retries",
                    extra={"event_id": str(event.id), "error": str(exc)},
                )
                return

            processed_at = datetime.now(timezone.utc)
            await db.mark_completed(event.id, processed_at)
            latency = (processed_at - event.produced_at).total_seconds()
            PROCESSING_LATENCY_SECONDS.observe(latency)
            EVENTS_PROCESSED.labels(event_type=event.event_type).inc()
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

            tasks = []
            for _tp, messages in batches.items():
                for msg in messages:
                    try:
                        event = Event.from_kafka_json(msg.value)
                    except Exception:
                        logger.exception("failed to parse message, skipping", extra={"offset": msg.offset})
                        continue
                    tasks.append(handle_message(event, dlq_producer, semaphore))

            if tasks:
                await asyncio.gather(*tasks)

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
