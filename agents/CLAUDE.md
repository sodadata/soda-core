# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Soda Core v4 is a data quality and data contract verification engine. It uses a plugin-based monorepo architecture where `soda-core` provides the engine/CLI and each data source (postgres, snowflake, trino, etc.) is a separate package discovered at runtime via Python entry points.

## Common Commands

### Development Setup
```bash
uv sync --all-packages --group dev
```

### Running Tests
```bash
# All tests (defaults to postgres via TEST_DATASOURCE env var)
uv run pytest soda-tests/

# Specific test categories
uv run pytest soda-tests/tests/unit/
uv run pytest soda-tests/tests/integration/
uv run pytest soda-tests/tests/feature/

# Single test file
uv run pytest soda-tests/tests/unit/test_contract_schema.py

# Data source-specific tests
uv run pytest soda-postgres/tests/
uv run pytest soda-trino/tests/

# Run tests against a specific data source
TEST_DATASOURCE=trino uv run pytest soda-tests
```

### Linting / Formatting
```bash
uv run pre-commit run --all-files
```

Pre-commit runs: autoflake (unused imports), isort (import sorting, black profile), black (formatting, line-length=120).

## Architecture

### Monorepo Layout
- **`soda-core/`** — Core engine, CLI, contract verification, SQL dialect abstraction. The CLI entry point is `soda_core.cli.cli:execute`.
- **`soda-{datasource}/`** — Data source plugins (postgres, trino, snowflake, bigquery, databricks, duckdb, redshift, sqlserver, synapse, fabric, athena, sparkdf).
- **`soda-tests/`** — Shared test suite (unit, integration, feature, components). Test helpers live in `soda-tests/src/helpers/`.
- **`soda/`** — Legacy v3 implementations (reference only, do not modify).

### Plugin System
Data source plugins register via entry points in their `pyproject.toml`:
```toml
[project.entry-points."soda.plugins.data_source.postgres"]
PostgresDataSourceImpl = "soda_postgres.common.data_sources.postgres_data_source:PostgresDataSourceImpl"
```
Discovery happens in `soda_core/common/plugins.py`. No explicit registration code is needed — installing the package makes the plugin available.

### Data Source Implementation Pattern
Each data source plugin provides three classes following this structure inside `soda_{datasource}/common/data_sources/`:

1. **`{Name}DataSourceImpl(DataSourceImpl)`** — Factory: creates the dialect and connection. Declares a `model_class` linking to the Pydantic config model.
2. **`{Name}SqlDialect(SqlDialect)`** — SQL generation overrides for the specific database (schema introspection, type mapping, quoting, feature flags like `SUPPORTS_MATERIALIZED_VIEWS`).
3. **`{Name}DataSourceConnection(DataSourceConnection)`** — Connection lifecycle management.

The base classes live in `soda-core/src/soda_core/common/`.

### Core Modules
- **`soda_core/cli/`** — CLI argument parsing and command handlers (contract verify/publish/fetch, data-source create/test).
- **`soda_core/contracts/`** — Contract verification engine. `contract_verification_impl.py` is the main verification implementation. `api/` exposes the public Python API (`verify_contract`, `publish_contract`, etc.).
- **`soda_core/common/sql_dialect.py`** — Central SQL abstraction (~2400 lines) that all data sources extend.
- **`soda_core/model/`** — Pydantic v2 data models for data source configurations.

### Test Infrastructure
- `TEST_DATASOURCE` env var (default: `postgres`) controls which data source the shared tests run against.
- `DataSourceTestHelper` (in `soda-tests/src/helpers/`) manages test database setup/teardown per session.
- Unit tests run only for postgres. Integration tests run for all data sources in CI.
- `.env` file at repo root is loaded by test fixtures for local credentials.

### Version Management
All packages share the same version (currently `4.0.8rc0`). `tbump.toml` coordinates version bumps across all `pyproject.toml` files.
