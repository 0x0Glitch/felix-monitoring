.PHONY: help install test run-market run-user run-all stop clean logs

help:
	@echo "Felix Exchange Monitoring - Commands"
	@echo "===================================="
	@echo "install       - Install dependencies"
	@echo "test          - Run integration tests"
	@echo "run-market    - Run market monitoring only"
	@echo "run-user      - Run user monitoring only"
	@echo "run-all       - Run both services"
	@echo "stop          - Stop all services"
	@echo "clean         - Clean temporary files"
	@echo "logs          - Show service logs"

install:
	@echo "Installing dependencies..."
	@pip install asyncpg aiohttp websockets psycopg2-binary python-dotenv requests

test:
	@echo "Running integration tests..."
	@python -m user_monitoring.test_integration

run-market:
	@echo "Starting market monitoring..."
	@python main.py

run-user:
	@echo "Starting user monitoring..."
	@python -m user_monitoring.main

run-all:
	@echo "Starting all services..."
	@docker-compose up -d

stop:
	@echo "Stopping services..."
	@docker-compose down

clean:
	@echo "Cleaning up..."
	@rm -rf __pycache__ user_monitoring/__pycache__
	@rm -rf data/*.tmp
	@find . -name "*.pyc" -delete

logs:
	@docker-compose logs -f --tail=100
