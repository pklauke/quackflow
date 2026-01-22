# Default recipe
default:
    @just --list

# Install dependencies
setup:
    uv sync --all-extras

# Run tests
test *args:
    uv run pytest {{args}}

# Run linter
lint:
    uv run ruff check src tests

# Run formatter
fmt:
    uv run ruff format src tests

# Run formatter check (no changes)
fmt-check:
    uv run ruff format --check src tests

# Run type checker
typecheck:
    uv run pyright src

# Run all checks (lint, format check, typecheck, tests)
check: lint fmt-check typecheck test

# Fix linting issues
fix:
    uv run ruff check --fix src tests

# Build package
build:
    uv build

# Clean build artifacts
clean:
    rm -rf dist/ build/ *.egg-info src/*.egg-info .pytest_cache .ruff_cache

# Start Kafka and Schema Registry for integration tests
kafka-up:
    docker-compose up -d
    @echo "Waiting for services to be healthy..."
    @sleep 5

# Stop Kafka and Schema Registry
kafka-down:
    docker-compose down -v

# Run integration tests (requires Kafka)
test-integration *args:
    uv run pytest tests/integration {{args}}

# Run unit tests only (excludes integration)
test-unit *args:
    uv run pytest tests --ignore=tests/integration {{args}}
