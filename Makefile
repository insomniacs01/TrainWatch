.PHONY: up down logs test lint local-up

PYTHON_BIN := $(shell if [ -x .venv/bin/python ]; then printf '%s' './.venv/bin/python'; elif command -v python3 >/dev/null 2>&1; then printf '%s' 'python3'; else printf '%s' 'python'; fi)

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f train-watch

test:
	$(PYTHON_BIN) -m unittest discover -s tests -p 'test_*.py'

lint:
	$(PYTHON_BIN) -m ruff check app tests run.py

local-up:
	./start.sh
