from prometheus_client import Counter, Gauge, Histogram

EVENTS_PRODUCED = Counter(
    "events_produced_total", "Events accepted by the API and published to Kafka", ["event_type"]
)
EVENTS_PROCESSED = Counter(
    "events_processed_total", "Events successfully processed by consumers", ["event_type"]
)
EVENTS_FAILED = Counter(
    "events_failed_total", "Event processing attempts that raised an error", ["event_type"]
)
EVENTS_DEAD_LETTERED = Counter(
    "events_dead_lettered_total", "Events that exhausted retries and were routed to the DLQ", ["event_type"]
)
EVENTS_DUPLICATE = Counter(
    "events_duplicate_total", "Events skipped because they were already processed (idempotency hit)", ["event_type"]
)

PROCESSING_LATENCY_SECONDS = Histogram(
    "event_processing_latency_seconds",
    "End-to-end latency from produce time to processed time",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

CONSUMER_LAG = Gauge(
    "consumer_group_lag", "Approximate consumer lag (messages) per partition", ["topic", "partition"]
)

IN_FLIGHT_EVENTS = Gauge("events_in_flight", "Events currently being processed by this consumer instance")

DB_WRITE_FAILURES = Counter(
    "db_write_failures_total",
    "Failed attempts to flush a batch to Postgres (retried with backpressure - see consumer.py)",
)
