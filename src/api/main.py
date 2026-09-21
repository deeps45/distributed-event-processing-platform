import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src import db
from src.common.logging_config import configure_logging
from src.monitoring.metrics import EVENTS_PRODUCED
from src.producer.producer import EventProducer
from src.redis_client import close_redis, get_redis
from src.schemas import Event, EventIn

configure_logging("api")
logger = logging.getLogger(__name__)

producer = EventProducer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pool()
    await producer.start()
    yield
    await producer.stop()
    await close_redis()
    await db.close_pool()


app = FastAPI(title="Distributed Event Processing Platform", lifespan=lifespan)


@app.post("/events", status_code=202)
async def submit_event(event_in: EventIn):
    event = Event(**event_in.model_dump())
    await db.insert_pending_event(event.id, event.event_type, event.payload, event.source, event.produced_at)
    await producer.send_event(event)
    EVENTS_PRODUCED.labels(event_type=event.event_type).inc()
    return {"id": str(event.id), "status": "accepted"}


@app.get("/events/{event_id}")
async def get_event(event_id: str):
    try:
        parsed_id = uuid.UUID(event_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="event_id must be a UUID")

    row = await db.get_event(parsed_id)
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return dict(row)


@app.get("/stats")
async def stats():
    rows = await db.count_by_status()
    return {row["status"]: row["count"] for row in rows}


@app.get("/health")
async def health():
    checks = {"postgres": False, "redis": False}
    try:
        await db.get_pool().fetchval("SELECT 1")
        checks["postgres"] = True
    except Exception:
        logger.exception("postgres health check failed")

    try:
        await get_redis().ping()
        checks["redis"] = True
    except Exception:
        logger.exception("redis health check failed")

    healthy = all(checks.values())
    return JSONResponse(
        content={"healthy": healthy, "checks": checks},
        status_code=200 if healthy else 503,
    )


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
