.PHONY: up down logs scale-consumers load benchmark test

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
	python3 scripts/benchmark.py

test:
	pytest -q
