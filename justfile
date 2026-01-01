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
