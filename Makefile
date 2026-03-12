.PHONY: up down logs test local-up

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f train-watch

test:
	python -m unittest discover -s tests -p 'test_*.py'

local-up:
	./start.sh
