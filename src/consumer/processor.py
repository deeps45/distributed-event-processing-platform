import random

from src.config import settings
from src.schemas import Event


class TransientProcessingError(Exception):
    """Simulates the kind of transient failure retry/DLQ logic exists for
    (a flaky downstream dependency, a timeout, etc.)."""


async def process_event(event: Event) -> None:
    """Business logic placeholder: validate the event and derive a result.

    SIMULATE_FAILURE_RATE injects synthetic failures so the retry -> backoff
    -> DLQ path can be exercised and benchmarked without needing to actually
    break Kafka/Postgres/Redis.
    """
    if settings.simulate_failure_rate > 0 and random.random() < settings.simulate_failure_rate:
        raise TransientProcessingError(f"simulated transient failure processing {event.id}")

    if not event.event_type:
        raise ValueError("event_type must not be empty")
