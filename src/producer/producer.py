import logging

from aiokafka import AIOKafkaProducer

from src.config import settings
from src.schemas import Event

logger = logging.getLogger(__name__)


class EventProducer:
    """Thin async wrapper around AIOKafkaProducer with idempotent, durable sends.

    - acks="all" + enable_idempotence: broker-side dedup and no acknowledged
      writes lost on a leader failover.
    - linger_ms batches concurrent sends into fewer, larger network requests,
      which is most of where the async pipeline's throughput/latency win
      over the synchronous baseline comes from (see scripts/benchmark.py).
    """

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            linger_ms=10,
            compression_type="gzip",
        )
        await self._producer.start()
        logger.info("producer started", extra={"bootstrap_servers": settings.kafka_bootstrap_servers})

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send_event(self, event: Event, topic: str | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("producer not started")
        await self._producer.send_and_wait(
            topic or settings.kafka_topic_events,
            key=str(event.id).encode("utf-8"),
            value=event.to_kafka_json(),
        )
