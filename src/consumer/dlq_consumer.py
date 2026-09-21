"""Standalone worker that tails the dead-letter topic.

Kept separate from the main consumer so DLQ volume (which should be near
zero in steady state) never competes for the same consumer group's
concurrency budget, and so alerting/audit logic can evolve independently of
the hot path.
"""

import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server

from src import db
from src.common.logging_config import configure_logging
from src.config import settings
from src.schemas import Event

configure_logging("dlq-consumer")
logger = logging.getLogger(__name__)


async def persist_dead_letter(event: Event, pool) -> None:
    await pool.execute(
        """
        INSERT INTO dead_letter_log (event_id, event_type, payload, error_context)
        VALUES ($1, $2, $3, $4)
        """,
        event.id,
        event.event_type,
        event.payload,
        f"retry_count={event.retry_count}",
    )
    # This is the natural hook point for paging/alerting (e.g. push a
    # CloudWatch metric or notify Slack) once DLQ volume crosses a threshold.
    logger.error("dead letter recorded", extra={"event_id": str(event.id), "event_type": event.event_type})


async def run() -> None:
    start_http_server(9101)

    await db.init_pool()
    pool = db.get_pool()

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_dlq,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"{settings.kafka_consumer_group}-dlq",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("dlq consumer started")

    try:
        async for msg in consumer:
            try:
                event = Event.from_kafka_json(msg.value)
            except Exception:
                logger.exception("failed to parse DLQ message, skipping", extra={"offset": msg.offset})
                await consumer.commit()
                continue

            await persist_dead_letter(event, pool)
            await consumer.commit()
    finally:
        await consumer.stop()
        await db.close_pool()


if __name__ == "__main__":
    asyncio.run(run())
