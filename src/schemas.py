import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventStatus(str, Enum):
    """The only two states ever persisted to Postgres - see
    src/consumer/consumer.py. Retries happen in-process before either of
    these is written, so there's no "pending"/"processing"/"retrying" row
    state to track; an event id with no row yet is just not done."""

    COMPLETED = "completed"
    DEAD_LETTER = "dead_letter"


class EventIn(BaseModel):
    """Payload accepted by the public API."""

    event_type: str
    payload: dict[str, Any]
    source: str = "api"


class Event(EventIn):
    """Canonical event envelope carried through Kafka and persisted to Postgres."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    produced_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0

    def to_kafka_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_json(cls, raw: bytes) -> "Event":
        return cls.model_validate_json(raw)
