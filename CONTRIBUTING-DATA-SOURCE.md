# Contribute support for a data source

Thanks for considering contributing to Soda Core's library of supported data sources!

This is a guide for **humans** who want to add a new data source. It walks through the moving parts, what you must implement, and how to test the result. If you'd rather have a denser, machine-oriented reference — every override, every signature — read [`CONTRIBUTING-DATA-SOURCE.AGENT.md`](CONTRIBUTING-DATA-SOURCE.AGENT.md). Coding agents (Claude, Cursor, …) should read both: this one for the *why*, the agent doc for the *what*.

There is no separate Soda SDK package. Extension support is built into Soda Core via Python entry points, a small set of base classes, and a handful of fully working reference packages. The existing data source packages **are** the SDK — when in doubt, copy from `soda-postgres/` (full featured) or `soda-duckdb/` (minimal).

---

## What you need to provide

- A **working data source** that reviewers can connect to:
    - Self-hostable engines (Postgres, MySQL, …): ship a `docker-compose.yml` with the package.
    - Cloud-only engines (BigQuery, Snowflake, Redshift, …): provide test credentials or a service account that CI can use.
- A **Python DB-API 2.0 (PEP 249) compatible driver** as a dependency. Soda Core calls into this driver to open connections and execute SQL.
- A **`soda-{name}/` package** that implements the three classes described below and registers itself through a Python entry point.

---

## Implementation basics

### Package layout

A new data source package mirrors the existing `soda-{name}/` siblings:

```
soda-{name}/
├── pyproject.toml                              # dependencies + entry point
├── docker-compose.yml                          # optional, for self-hostable engines
├── src/
│   └── soda_{name}/
│       ├── __init__.py
│       ├── common/
│       │   └── data_sources/
│       │       ├── {name}_data_source.py            # impl + dialect
│       │       └── {name}_data_source_connection.py # connection + pydantic models
│       └── test_helpers/
│           └── {name}_data_source_test_helper.py
└── tests/
    ├── conftest.py
    └── data_sources/
        └── test_{name}.py                      # data-source-specific smoke tests
```

The cleanest way to start is to copy `soda-duckdb/` (smallest) or `soda-postgres/` (full-featured) and rename everything from the original prefix to your `{name}`.

### The three classes

A data source is wired up by implementing three classes plus a pydantic connection model.

| Class | Base | Purpose |
|---|---|---|
| `{Name}DataSourceConnection` | `DataSourceConnection` | Wraps your DB-API driver. Implements `_create_connection()` and any retry / cursor quirks. |
| `{Name}SqlDialect` | `SqlDialect` | Tells Soda how to render SQL for your engine — type names, regex, casing, metadata queries. |
| `{Name}DataSourceImpl` | `DataSourceImpl` | The entry-point class. Wires the connection and dialect together. Two abstract methods. |

Plus a pydantic schema — `{Name}DataSource(DataSourceBase)` and one or more `{Name}ConnectionProperties` variants (typically one per authentication method) — that defines the YAML users will write in their `data-source.yml`. Look at `PostgresConnectionPropertiesBase` and the three concrete variants (`PostgresConnectionString`, `PostgresConnectionPassword`, `PostgresConnectionPasswordFile`) for the canonical multi-auth pattern.

The `type: Literal["{name}"]` field on your `DataSourceBase` subclass is the value users put in YAML *and* the suffix of your entry-point group. They must match.

### Required overrides on `SqlDialect`

`SqlDialect` has ~150 hooks but most defaults are sane. In practice you must override:

- `get_data_source_data_type_name_by_soda_data_type_names()` — Soda canonical type names → native DDL type names (used when creating tables).
- `get_soda_data_type_name_by_data_source_data_type_names()` — native type names → Soda canonical type names (used by schema checks).
- `get_database_prefix_index()` / `get_schema_prefix_index()` — how `dataset_prefix` is interpreted for this engine. Postgres uses `[database, schema]` (`0`, `1`). DuckDB has no database concept, so returns `None` for database, `0` for schema.
- `_build_regex_like_sql(...)` — every dialect spells regex differently. Postgres: `expr ~ 'pattern'`. DuckDB: `REGEXP_MATCHES(expr, 'pattern')`. Snowflake: `REGEXP_LIKE(expr, 'pattern')`.

You must also pass the matching sqlglot dialect via the subclass keyword:

```python
class {Name}SqlDialect(SqlDialect, sqlglot_dialect="{sqlglot_name}"):
    ...
```

### Optional overrides — frequently needed

- `_build_cast_sql(...)` — Postgres uses `expr::type`; everyone else uses `CAST(expr AS type)` (the default).
- `escape_string(...)` / `escape_regex(...)` — quoting and escaping rules.
- `quote_default(...)` / `quote_for_ddl(...)` — identifier quoting (default: double-quote).
- `default_casify(...)` / `metadata_casify(...)` — case normalisation. Postgres lowercases unquoted identifiers; Snowflake uppercases.
- `_get_data_type_name_synonyms()` — list of equivalent native type names (e.g. `["varchar", "character varying"]`).
- `supports_data_type_*()` and `default_numeric_precision()` / `default_numeric_scale()` — for engines that don't expose precision/scale via metadata.
- `build_columns_metadata_query_str(...)` / `build_all_columns_metadata_query_str(...)` — defaults query `INFORMATION_SCHEMA`. Override if your engine doesn't have one. See `postgres_data_source.py` for an example that builds queries with the `sql_ast` DSL against `pg_catalog`.

### Optional overrides — infrequent

- `create_schema_if_not_exists_sql(...)` — for engines with non-standard schema DDL.
- `supports_materialized_views()`, `supports_sampler(...)`, `_build_sample_sql(...)` — only if you support these features.
- `bulk_columns_metadata_available` on `DataSourceImpl` — set to `False` if your engine can only fetch column metadata one table at a time (e.g. `DESCRIBE TABLE` per table).
- `create_metadata_tables_query()` — override on `DataSourceImpl` if you need a custom subclass of `MetadataTablesQuery`.

### Connection class

```python
class {Name}DataSourceConnection(DataSourceConnection):
    def __init__(self, name, connection_properties):
        super().__init__(name, connection_properties)

    def _create_connection(self, config):
        return {your_dbapi}.connect(**config.to_connection_kwargs())
```

Override `execute_query` / `execute_update` only if the driver needs specific error handling. The Postgres connection rolls back the transaction on failure — see `PostgresDataSourceConnection.execute_query`.

If your driver's cursor description rows aren't named tuples, override `_execute_query_get_result_row_column_name`. DuckDB wraps the cursor to get DB-API compatibility — read its code if your driver doesn't behave like Postgres'.

### Further considerations

- **Schemas.** Some engines don't have schemas (DuckDB), some require them to be prefixed in every query (Snowflake), and some can have a default schema set on the connection. Make sure your `*_prefix_index` overrides reflect this.
- **Case sensitivity.** Postgres folds unquoted identifiers to lowercase. Snowflake folds to uppercase. Quoted identifiers are preserved. Reflect this in `default_casify` and `metadata_casify`.
- **Identifier quoting.** Default is `"double quotes"`. BigQuery uses backticks. Override `quote_default` if needed.
- **Regex.** Even engines that all "support regex" express it differently. Don't assume the default works.

---

## Wire it up

In `soda-{name}/pyproject.toml`:

```toml
[project]
name = "soda-{name}"
version = "<match soda-core version>"
requires-python = ">=3.10"
dependencies = [
    "soda-core==<match>",
    "{your-dbapi-driver}>=X.Y",
]

[project.entry-points."soda.plugins.data_source.{name}"]
{Name}DataSourceImpl = "soda_{name}.common.data_sources.{name}_data_source:{Name}DataSourceImpl"

[tool.uv.sources]
soda-core = { workspace = true }

[build-system]
requires = ["setuptools>=45", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools]
package-dir = {"" = "src"}
```

The entry-point group `soda.plugins.data_source.{name}` is what makes your package discoverable — there is no central registry. Loading an entry point triggers `DataSourceImpl.__init_subclass__(model_class=...)`, which registers the impl class keyed by the `Literal[...]` default on your `DataSourceBase` subclass. At runtime, Soda matches the `type:` field in the user's YAML against that registry. By convention the entry-point `{name}` suffix matches the `Literal[...]` value, but the load-bearing match is YAML `type:` ↔ `Literal[...]` — not the entry-point name. Keep all three aligned for sanity.

Then add `soda-{name}` to the workspace `pyproject.toml` (`[tool.uv.workspace] members`) and your `tests/` path to `pytest.ini` (`testpaths` and `pythonpath`).

---

## Test the data source

There are two test layers, and a serious contribution exercises both.

### Data-source-specific tests

Put anything genuinely unique to your engine — connection edge cases, dialect quirks, dependency behaviour — under `soda-{name}/tests/data_sources/test_{name}.py`. Most data-source-specific test files are short.

### Cross-data-source test suite

The shared suite under `soda-tests/tests/` runs **the same tests** against every registered data source. This is where coverage actually comes from. To plug your data source in:

1. Implement a `{Name}DataSourceTestHelper(DataSourceTestHelper)` under `src/soda_{name}/test_helpers/`. At minimum override `_create_data_source_yaml_str()` to render a valid YAML config. Override `_create_database_name()` / `_create_schema_name()` if your engine has special naming rules.
2. Register your helper inside `DataSourceTestHelper.create()` in `soda-tests/src/helpers/data_source_test_helper.py` — add an `elif` branch for your `{name}`.
3. Add your `tests/` path to `testpaths` and `pythonpath` in `pytest.ini`.

To run the full suite against your engine:

```bash
TEST_DATASOURCE={name} uv run pytest soda-tests/tests
```

Credentials live in a `.env` file (kept out of git) — see `.env_example`.

---

## Development setup

```bash
# from repo root
uv sync --all-packages --group dev

# format + lint
uv run pre-commit run --all-files

# unit tests (don't need a database)
uv run pytest soda-tests/tests/unit

# your data source tests
uv run pytest soda-{name}/tests
```

---

## Submitting

1. Open a draft PR early. Soda maintainers can advise on tricky dialect issues before you sink time into them.
2. Include in the PR description:
    - which DB version(s) you tested against,
    - any features you deliberately did not implement, and
    - any tests you skipped, with reasons.
3. Make sure the cross-data-source suite passes: `TEST_DATASOURCE={name} uv run pytest soda-tests/tests`.

If you get stuck, the [agent reference](CONTRIBUTING-DATA-SOURCE.AGENT.md) catalogues every override, every common failure mode, and the verification commands maintainers will run on your PR. It's intentionally written so a coding agent can drive the implementation end-to-end with a human reviewing.
