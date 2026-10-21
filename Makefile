.PHONY: install lint test up down logs help

VENV ?= .venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip
RUFF = $(VENV)/bin/ruff
PYTEST = $(VENV)/bin/pytest

help:
	@echo "Available Makefile targets:"
	@echo "  install  - Create virtual environment and install pinned dependencies"
	@echo "  lint     - Run ruff linter and style checks"
	@echo "  test     - Run pytest test suite"
	@echo "  up       - Build images and start all docker compose services in background"
	@echo "  down     - Stop and remove all docker compose services"
	@echo "  logs     - Tail logs from all docker compose services"

install:
	@if [ ! -d "$(VENV)" ]; then python3 -m venv $(VENV); fi
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

lint:
	@if [ -x "$(RUFF)" ]; then $(RUFF) check .; else ruff check .; fi

test:
	@if [ -x "$(PYTEST)" ]; then $(PYTEST); else pytest; fi

up:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f
