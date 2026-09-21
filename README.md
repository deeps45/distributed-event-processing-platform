# Distributed Event Processing Platform

A distributed, fault-tolerant event processing pipeline: async Kafka producers/consumers,
a REST ingestion API, Redis-backed idempotency, Postgres persistence, Prometheus
monitoring, and a dead-letter queue for failure recovery — all containerized, with
Terraform to deploy it to AWS on a free-tier-safe footprint.

## Architecture

```
                 ┌─────────────┐
  HTTP POST ───▶ │   FastAPI   │──────┐
  /events        │  (api)      │      │  produce (async, batched, acks=all)
                 └──────┬──────┘      ▼
                        │        ┌─────────┐
                 insert │        │  Kafka  │
                 pending│        │ (events)│
                        ▼        └────┬────┘
                 ┌─────────────┐      │ consume (concurrent, manual commit)
                 │  PostgreSQL │      ▼
                 │  (events,   │ ┌──────────────┐     idempotency check
                 │ dead_letter │◀│   Consumer   │◀──── (Redis SETNX)
                 │    _log)    │ │ (N replicas) │
                 └─────────────┘ └──────┬───────┘
                        ▲               │ retry w/ backoff,
                        │               │ then DLQ on exhaustion
                        │               ▼
                        │        ┌─────────────┐      ┌─────────────┐
                        └────────│ events.dlq  │─────▶│ DLQ Consumer│
                                 │   (Kafka)   │      │  (audit log)│
                                 └─────────────┘      └─────────────┘

  Prometheus scrapes /metrics on api + every consumer replica.
```

## What's in here, mapped to the design goals

- **Distributed Kafka consumers + Python services on Docker (1M+ events/day)** —
  `src/consumer/consumer.py` runs as N independent replicas in the same consumer
  group (`docker compose up --scale consumer=3`); Kafka handles partition
  assignment. See [Benchmarks](#benchmarks) for measured throughput.
- **Async producers, consumers, REST APIs; -40% latency** — `src/producer/producer.py`
  uses `aiokafka` with `linger_ms` batching and idempotent, concurrent sends instead
  of blocking one-at-a-time calls; `src/api/main.py` is a fully async FastAPI service;
  `src/consumer/consumer.py` processes a batch's messages concurrently
  (`asyncio.gather`, bounded by a semaphore) instead of sequentially. See
  [Benchmarks](#benchmarks) for the measured reduction vs. a synchronous baseline.
- **Fault-tolerant retry, persistence, monitoring, failure-recovery** —
  `src/common/retry.py` (exponential backoff + a circuit breaker),
  `events.dlq` + `src/consumer/dlq_consumer.py` (dead-letter queue with an audit
  trail in `dead_letter_log`), Redis-backed idempotency so Kafka's at-least-once
  delivery doesn't double-process on redelivery, and Prometheus metrics
  (`events_processed_total`, `events_failed_total`, `events_dead_lettered_total`,
  `event_processing_latency_seconds`) exposed by every service.

## Quickstart (local, $0, no AWS account needed)

Requires Docker Desktop.

```bash
make up              # builds images, starts Kafka/Redis/Postgres/api/consumer/dlq-consumer/prometheus
curl -X POST localhost:8000/events \
  -H 'content-type: application/json' \
  -d '{"event_type": "order.created", "payload": {"amount": 42.50}}'
curl localhost:8000/stats
curl localhost:8000/metrics | grep events_processed_total
```

- API: http://localhost:8000 (`/docs` for interactive OpenAPI UI)
- Prometheus: http://localhost:9090
- Generate realistic traffic: `make load` (5,000 events via the REST API)
- Scale out consumers: `make scale-consumers`
- Run tests: `make test`
- Tear down: `make down`

To see the retry → DLQ path trigger, `SIMULATE_FAILURE_RATE` in
`docker-compose.yml` injects a configurable rate of synthetic transient
failures into `src/consumer/processor.py` (default 5%) — that's the hook
point where real business logic would go.

## Benchmarks

`scripts/benchmark.py` is a producer-level A/B test: N events sent one at a
time with a blocking `send_and_wait()` per event (the "synchronous" baseline)
vs. the same N events sent concurrently through the platform's actual
producer config (`linger_ms=10`, `acks=all`, `enable_idempotence=True`,
bounded concurrency). It measures wall-clock latency directly — nothing here
is hardcoded or simulated.

```bash
pip install -r requirements.txt
docker compose up -d kafka
python3 scripts/benchmark.py
```

<!-- BENCHMARK_RESULTS -->

Reproduce this yourself — results depend on your machine and should be
re-run before being cited anywhere.

## Fault tolerance in detail

- **Retries**: `src/common/retry.py::retry_with_backoff` — exponential backoff
  with jitter, `MAX_RETRIES` attempts (default 5), all in-process before the
  consumer gives up on a message.
- **Dead-letter queue**: on exhausting retries, the event is published to
  `events.dlq`, the source row in `events` is marked `dead_letter`, and a
  separate `dlq-consumer` service persists an immutable audit record to
  `dead_letter_log` — the natural hook point for paging/alerting.
- **Idempotency**: Kafka only guarantees at-least-once delivery; a consumer
  crash after processing but before committing an offset will redeliver a
  message. `src/redis_client.py::mark_processed_if_new` uses `SET NX EX` to
  claim an event id exactly once, making reprocessing a no-op — this is what
  turns "at least once" into "effectively once" without needing a
  transactional outbox.
- **Circuit breaker**: `src/common/retry.py::CircuitBreaker` is available for
  wrapping calls to a downstream dependency that's failing consistently
  (rather than transiently), to fail fast instead of retrying into a
  degraded system.

## Monitoring

Every service (`api`, each `consumer` replica, `dlq-consumer`) exposes
Prometheus metrics. Key series: `events_produced_total`,
`events_processed_total`, `events_failed_total`,
`events_dead_lettered_total`, `events_duplicate_total` (idempotency hits),
`event_processing_latency_seconds` (histogram), `events_in_flight`.
`monitoring/prometheus.yml` wires up scraping for the default (unscaled)
compose topology; horizontally scaling monitoring past a single consumer
replica in a real deployment would use Prometheus service discovery
(ECS/K8s SD) rather than the static config here.

## AWS deployment (optional, your account, your cost)

`infra/` is Terraform for a **free-tier-safe** deployment: one
free-tier-eligible EC2 instance (`t3.micro` by default) that self-hosts
Kafka + Redis + Postgres via the same `docker-compose.yml` used locally.

**Why not AWS's managed services?** RDS Postgres and EC2 have genuine free
tiers (750 hrs/month for 12 months on a new account). **AWS MSK (managed
Kafka) does not** — a minimal MSK cluster typically runs $100+/month. Running
Kafka in Docker on the free-tier EC2 instance instead keeps this at $0 as
long as you stay within the free-tier hour allowance and terminate the
instance when you're not using it.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # fill in your key pair name and IP
terraform init
terraform plan     # review what this will create before applying
terraform apply
# ... when you're done:
terraform destroy  # IMPORTANT - avoids ongoing charges
```

I did not run this against any AWS account from this session — it requires
your own AWS credentials, which should never be pasted into a chat. Run it
yourself, and set an AWS Budget alert (Billing → Budgets) before applying if
you want a safety net.

## Project layout

```
src/
  api/main.py            REST API (submit/query events, health, metrics)
  producer/producer.py   Async, batched, idempotent Kafka producer
  consumer/consumer.py   Concurrent consumer: idempotency, retry, DLQ routing
  consumer/dlq_consumer.py  Dead-letter audit trail
  consumer/processor.py  Business logic hook (+ synthetic failure injection)
  common/retry.py        Exponential backoff, circuit breaker
  monitoring/metrics.py  Prometheus metric definitions
  db.py, redis_client.py, config.py, schemas.py, aws_integration.py
scripts/
  benchmark.py           Producer latency/throughput A/B benchmark
  generate_load.py       Realistic end-to-end load generator
infra/                   Terraform for the free-tier AWS deployment
migrations/001_init.sql  Postgres schema
```
