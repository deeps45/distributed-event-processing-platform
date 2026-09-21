.PHONY: up down logs scale-consumers load benchmark capacity-test chaos-test test

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f api consumer dlq-consumer

scale-consumers:
	docker compose up -d --scale consumer=3

load:
	python3 scripts/generate_load.py --total 5000 --concurrency 100

benchmark:
	PYTHONPATH=. python3 scripts/benchmark.py

capacity-test:
	PYTHONPATH=. python3 scripts/capacity_test.py --total 50000 --concurrency 200

# Needs at least 2 consumer replicas (docker compose up -d --scale consumer=3)
# so killing one still leaves survivors to take over its partitions.
chaos-test:
	PYTHONPATH=. python3 scripts/chaos_test.py --total 20000 --kill-at 5000

test:
	pytest -q
