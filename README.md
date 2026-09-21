# Distributed Event Processing Platform

A distributed, fault-tolerant event processing pipeline: async Kafka producers/consumers,
a REST ingestion API, Redis-backed idempotency, Postgres persistence, Prometheus
monitoring, and a dead-letter queue for failure recovery — all containerized, with
Terraform to deploy it to AWS on a free-tier-safe footprint.

**What I'd do differently with more time:** run a real 3-broker Kafka
cluster with actual replication instead of single-broker KRaft, so
broker-failure recovery is something this repo actually tests instead of
assumes; replace the hand-rolled load generator with a real tool (k6 or
vegeta) — debugging my own test client's throughput ceiling, not the
pipeline's, cost real time during benchmarking (see
[Benchmarks](#benchmarks)); and wire DLQ volume to an actual alert
(Slack/PagerDuty) instead of a log line, since a hook point existing isn't
the same as someone getting paged.

## Architecture

```
                 ┌─────────────┐
  HTTP POST ───▶ │   FastAPI   │──────┐
  /events        │  (api)      │      │  produce (async, batched, acks=all)
                 └─────────────┘      ▼           no DB write here - see below
                                  ┌─────────┐
                                  │  Kafka  │  6 partitions
                                  │ (events)│
                                  └────┬────┘
                                       │ consume (concurrent, manual commit)
                                       ▼
                 ┌─────────────┐ ┌──────────────┐     idempotency check,
                 │  PostgreSQL │◀│   Consumer   │◀──── batched (Redis pipeline,
                 │  (events,   │ │ (N replicas, │      SETNX per key)
                 │ dead_letter │ │ 2 partitions │
                 │    _log)    │ │  each @N=3)  │
                 └─────────────┘ └──────┬───────┘
                        ▲               │ retry w/ backoff,
                        │               │ then DLQ on exhaustion
                        │               ▼
                        │        ┌─────────────┐      ┌─────────────┐
                        └────────│ events.dlq  │─────▶│ DLQ Consumer│
                          batched│   (Kafka)   │      │  (audit log)│
                          write  └─────────────┘      └─────────────┘

  Prometheus scrapes /metrics on api + every consumer replica.
```

**Write path, and why it's shaped this way:** the API does not write to
Postgres — it only produces to Kafka and returns. The consumer is the sole
writer, and it writes once per event, not three times: earlier versions of
this service had the API insert a `pending` row, the consumer update it to
`processing`, then update it again to `completed` — three sequential
Postgres round trips in the critical path of every single event. That's
gone. The consumer now also batches: it collects every outcome from one
`getmany()` poll (up to 500 events) and writes them in a single
`INSERT ... FROM unnest(...)` statement per outcome type (completed /
dead-lettered), instead of one write per event. Same for the Redis
idempotency check — one pipelined round trip per poll batch, not one per
event. This is what actually moved the latency and throughput numbers below
(see [Benchmarks](#benchmarks)); Kafka itself was never the bottleneck.

The trade-off: `GET /events/{id}` is eventually consistent. Right after a
`POST /events` returns 202, the event may not be queryable yet (404) until
the consumer catches up - typically tens of milliseconds, but not
instantaneous. That's a deliberate choice, not an oversight; a system
tracking "pending" state for every in-flight event at real volume pays for
it in write amplification on the one component (Postgres) least able to
scale horizontally here.

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

Three separate, real measurements — reproduce them yourself before citing
any of these numbers anywhere; all depend on the machine they're run on,
and this is one laptop running Docker Desktop, not a real multi-node
cluster (see [Honest limits](#honest-limits-of-this-testing) below).

### 1. Producer latency: sync baseline vs. the platform's async pipeline

`scripts/benchmark.py` sends N events one at a time with a blocking
`send_and_wait()` per event (the "synchronous" baseline), then sends the
same N events concurrently through the platform's actual producer config
(`linger_ms=10`, `acks=all`, `enable_idempotence=True`, 100-way concurrency).
"Effective latency" is wall-clock time to fully drain the batch, divided by
event count — the apples-to-apples number across both modes (the raw
per-call round-trip time isn't comparable between modes on its own, since
under concurrency it includes time spent queued behind other in-flight
sends; see the script's comments).

```bash
pip install -r requirements.txt
docker compose up -d kafka
PYTHONPATH=. python3 scripts/benchmark.py
```

Measured run (2,000 events, M4 MacBook Pro, local Docker Kafka):

| | effective latency / event | throughput |
|---|---|---|
| Sync baseline (one `send_and_wait()` at a time) | 0.64 ms | 1,553 events/sec |
| Async pipeline (batched, 100-way concurrent) | 0.13 ms | 7,695 events/sec |

**Effective per-event latency reduction: 79.8%.**

### 2. Full pipeline via the REST API — and a bottleneck I found in my own test tool

`make load` (`scripts/generate_load.py`) drives real traffic through the
public API end-to-end: 30,000 events, 3 consumer replicas, `SIMULATE_FAILURE_RATE=0.05`
exercising the retry path live. Measured: **173 events/sec**, zero data loss
(80,000/80,000 events landed in Postgres across this and an earlier run),
zero growing backlog (Kafka consumer lag stayed under ~110 messages the
entire run, checked via `kafka-consumer-groups.sh --describe`).

That number is real, but it's not the pipeline's ceiling — it's
`generate_load.py`'s ceiling. Consumer lag never grew during the run, which
means the consumers were idle waiting on events, not the other way around:
the bottleneck was a single Python process's `httpx.AsyncClient` making
30,000 HTTP round trips, not Kafka, Postgres, or the consumers. Measurement
#3 isolates the part that actually matters.

### 3. Consumer pipeline capacity, isolated from the test tool

`scripts/capacity_test.py` publishes directly to Kafka (same path the API's
producer takes, minus the HTTP hop — so it's still exercising the real
producer config), then waits for the 3-replica consumer group to fully
drain the batch and computes throughput from Postgres's own `processed_at`
timestamps (min/max across the batch), not wall-clock guesses.

```bash
docker compose up -d --build --scale consumer=3
PYTHONPATH=. python3 scripts/capacity_test.py --total 50000 --concurrency 200
```

Measured run (50,000 events, 3 consumer replicas, 6 Kafka partitions):
- Produced 50,000 events to Kafka in 3.64s (13,722/sec — the producer
  alone; see benchmark #1 for why this isn't the pipeline number either).
- **Consumer pipeline sustained 1,653 events/sec** end-to-end into
  Postgres, measured over the full 50,000-event drain window.
- **Extrapolated: ~142.8M events/day** — about 143x the 1M+/day target.
- Verified afterward: consumer group lag was exactly 0 on all 6 partitions,
  and Postgres held exactly 80,000 completed rows (30,000 from run #2 +
  50,000 from this run) — no loss, nothing stuck.

This is the number I'd actually stand behind for a "sustained throughput"
claim: it isolates the pipeline (Kafka → 3 consumer replicas → batched
Postgres writes) from any test client's own limits, and every event in it
is independently verifiable in Postgres.

### Horizontal scaling

`KAFKA_NUM_PARTITIONS=6` on the broker means `docker compose up --scale
consumer=N` for N up to 6 actually redistributes partitions across
replicas — verified via `kafka-consumer-groups.sh --describe`: with 3
replicas running, each held exactly 2 of the 6 partitions. A single
partition can only be consumed by one consumer in a group at a time, so
this was a real fix, not cosmetic — the very first version of this repo had
only 1 partition, which meant scaling consumer replicas did nothing.

### Honest limits of this testing

- One machine, Docker Desktop, not a real multi-broker Kafka cluster, not
  separate hardware for Postgres/Redis/Kafka. Numbers on real distributed
  infrastructure (see [AWS deployment](#aws-deployment-optional-your-account-your-cost))
  will differ in both directions — likely better sustained throughput with
  dedicated resources, but also real network latency between services that
  loopback networking here doesn't have.
- "Sustained" here means a 30-second drain window, not a 24-hour run. The
  methodology (throughput while consumer lag holds flat or hits zero) is
  the standard way to validate a sustained-rate claim without literally
  running for a day, but it's still a few tens of seconds, not endurance
  testing.
- `SIMULATE_FAILURE_RATE=0.05` is synthetic. Real failure modes (a slow
  downstream dependency, a poison-pill message, a full disk) will behave
  differently than a coin flip in `processor.py`.

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
  generate_load.py       Realistic end-to-end load generator (via the REST API)
  capacity_test.py       Consumer pipeline sustained-throughput test, isolated
                          from the load generator's own limits
infra/                   Terraform for the free-tier AWS deployment
migrations/001_init.sql  Postgres schema
```
