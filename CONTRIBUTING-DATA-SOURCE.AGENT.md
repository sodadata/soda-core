# Adding a data source or check type — agent reference

Dense reference for coding agents implementing a new Soda Core data source or
check type. Pair with [`CONTRIBUTING-DATA-SOURCE.md`](CONTRIBUTING-DATA-SOURCE.md)
for context. This document is intentionally written so a coding agent can
drive the implementation end-to-end with a human reviewing — the human guide
gives the *why*, this one gives the *what* (every override, every signature,
every failure mode).

Every symbol mentioned here is real and resolvable in this repository as of
writing.

> **Source of truth ordering:** when this guide and the actual code disagree,
> the code wins. Re-read the base classes; this document may have drifted.

## Contents

- §0 Operating contract
- §1 Base classes (read these before coding)
- §2 Adding a data source — step by step
- §3 Adding a check type — step by step
- §4 SQL-AST builders cheat sheet
- §5 Plugin discovery — how it actually works
- §6 Verification
- §7 Common failure modes
- §8 What you do **not** need to do
- §9 Pointers to canonical examples

## Runtime flow (orientation)

```
$ soda contract verify -ds ds.yml -c contract.yml
        │
        ▼
  cli.py  ──►  load_plugins()
                  │
                  │  iterates entry_points() under soda.plugins.*
                  │  each ep.load() imports the target class
                  │  importing a *DataSourceImpl class triggers
                  │  DataSourceImpl.__init_subclass__(model_class=...)
                  │  which registers it keyed by model_class.get_class_type()
                  ▼
  parse YAML (data source + contract)
        │
        ▼
  DataSourceImpl.from_yaml_source(yaml)
        │   reads `type:` from YAML, looks up registered impl class,
        │   instantiates {Name}DataSourceImpl(data_source_model=...)
        ▼
  {Name}DataSourceImpl.__init__
        ├── _create_sql_dialect()           → {Name}SqlDialect()
        └── _create_data_source_connection() → {Name}DataSourceConnection(...)
        │
        ▼
  ContractVerification.execute()
        │   sql_ast builders + dialect.build_*_sql(...) → SQL string
        │   connection.execute_query(sql) → rows + cursor.description
        │   native types canonicalised via dialect's type-name dicts
        ▼
  CheckImpl.evaluate(measurement_values) → CheckResult
```

---

## 0. Operating contract

You will:

1. Read the base classes listed in §1 before writing anything.
2. Copy an existing implementation (DuckDB = minimal, Postgres = full featured)
   and rename, **then** adapt. Do not generate from scratch.
3. Use existing implementations as templates. Postgres = full featured,
   DuckDB = minimal, Snowflake/Databricks = warehouse semantics.
4. Run the verification commands in §6 before claiming completion. Do not
   declare a task done unless tests actually pass.

If you are blocked on a single override (e.g. how this dialect spells regex),
**read another dialect implementation** before guessing. There are a dozen
existing dialect packages in this monorepo to compare against.

---

## 1. Base classes (read these before coding)

| Class | File | What you must know |
|---|---|---|
| `DataSourceImpl` | `soda-core/src/soda_core/common/data_source_impl.py` | Two abstract methods. Auto-registers via `__init_subclass__(model_class=...)`. |
| `DataSourceConnection` | `soda-core/src/soda_core/common/data_source_connection.py` | Abstract `_create_connection()`. Default `execute_query` uses cursor.fetchall + cursor.description. |
| `SqlDialect` | `soda-core/src/soda_core/common/sql_dialect.py` | ~150 hooks. Most have sane defaults. The required overrides are the type-name dicts and (if the SQL is non-standard) regex/cast/sample. |
| `DataSourceBase` | `soda-core/src/soda_core/model/data_source/data_source.py` | Pydantic `BaseModel` + `ConfigDict(frozen=True, extra="forbid")`. `type: Literal["{name}"]` is what the plugin loader matches against. |
| `DataSourceConnectionProperties` | `soda-core/src/soda_core/model/data_source/data_source_connection_properties.py` | Base for your YAML connection schema. |
| `CheckParser`, `CheckImpl`, `CheckYamlParser`, `CheckYaml` | `contracts/impl/contract_yaml.py` + `contract_verification_impl.py` | Two register-by-classmethod pairs. |
| `MetricImpl`, `AggregationMetricImpl`, `DerivedMetricImpl` | `contract_verification_impl.py` (grep `^class MetricImpl` to locate) | `AggregationMetricImpl.sql_expression()` returns a `SqlExpression` from `sql_ast`. |
| `Plugin` (Protocol) | `soda-core/src/soda_core/plugins.py` | `setup_cli(parser)` + `load()` classmethods. Discovered via `entry_points()` under `soda.plugins.*`. |
| `DataSourceTestHelper` | `soda-tests/src/helpers/data_source_test_helper.py` | Override `_create_data_source_yaml_str` (required) + database/schema name methods (often). |

---

## 2. Adding a data source — step by step

### 2.1. Package skeleton

Copy `soda-duckdb/` (smallest reference) and rename `duckdb` → `{name}` in
file/dir names and module paths. Final structure:

```
soda-{name}/
├── pyproject.toml
├── docker-compose.yml          # only if applicable
├── src/soda_{name}/
│   ├── __init__.py
│   ├── common/data_sources/
│   │   ├── __init__.py
│   │   ├── {name}_data_source.py
│   │   └── {name}_data_source_connection.py
│   └── test_helpers/
│       ├── __init__.py
│       └── {name}_data_source_test_helper.py
└── tests/
    ├── conftest.py
    └── data_sources/
        └── test_{name}.py
```

### 2.2. Connection model (pydantic)

In `{name}_data_source_connection.py`:

```python
class {Name}ConnectionProperties(DataSourceConnectionProperties, ABC):
    field_mapping: ClassVar[Dict[str, str]] = {
        # canonical name in YAML -> driver kwarg name
        # only needed if names differ; example: postgres maps "database" -> "dbname"
    }

class {Name}ConnectionPropertiesBase({Name}ConnectionProperties, ABC):
    host: str = Field(..., description="...")
    port: int = Field(..., ge=1, le=65535)
    # ... your fields with pydantic Field(..., description=...)

# One concrete subclass per auth method — most real engines have 2-3:
class {Name}ConnectionString({Name}ConnectionProperties):
    connection_string: SecretStr = Field(..., description="...")

class {Name}ConnectionPassword({Name}ConnectionPropertiesBase):
    password: SecretStr = Field(..., description="...")

class {Name}ConnectionPasswordFile({Name}ConnectionPropertiesBase):
    password_file: str = Field(..., description="path to file holding the password")

class {Name}DataSource(DataSourceBase, ABC):
    type: Literal["{name}"] = Field("{name}")
    connection_properties: {Name}ConnectionProperties = Field(
        ..., alias="connection", description="..."
    )

    @field_validator("connection_properties", mode="before")
    def infer_connection_type(cls, value):
        # discriminate between auth variants based on present keys
        if "connection_string" in value:
            return {Name}ConnectionString(**value)
        if "password_file" in value:
            return {Name}ConnectionPasswordFile(**value)
        if "password" in value:
            return {Name}ConnectionPassword(**value)
        raise ValueError("Unknown connection structure")
```

See `soda-postgres/src/soda_postgres/common/data_sources/postgres_data_source_connection.py`
for the canonical three-variant pattern.

The `type` literal is the load-bearing match — the user's YAML `type:` field
is looked up against the registry keyed by this `Literal[...]` default. The
entry-point group's suffix should match by convention, but doesn't have to
(see §5).

### 2.3. `DataSourceConnection` subclass

```python
class {Name}DataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(self, config: {Name}ConnectionProperties):
        connection_kwargs = config.to_connection_kwargs()
        return {your_dbapi}.connect(**connection_kwargs)
```

Override `execute_query` / `execute_update` only if the driver requires
specific error handling (Postgres rolls back the transaction on failure;
see `PostgresDataSourceConnection`).

If your driver's cursor description rows don't have a `.name` attribute
(e.g. they're plain tuples), override `_execute_query_get_result_row_column_name`.
DuckDB demonstrates wrapping the cursor in a namedtuple to get DB-API
compatibility.

### 2.4. `SqlDialect` subclass

```python
class {Name}SqlDialect(SqlDialect, sqlglot_dialect="{sqlglot_name}"):
    SODA_DATA_TYPE_SYNONYMS = (
        # tuples of SodaDataTypeName members that should be considered equivalent
        # for schema-check purposes in this dialect
    )
```

The `sqlglot_dialect=` kwarg is **required** by `__init_subclass__`. Pick the
matching dialect from sqlglot, or `"postgres"` as a generic fallback.

`SodaDataTypeName` is the canonical Soda type-name enum defined in
`soda_core/common/metadata_types.py`. `python -c "from soda_core.common.metadata_types
import SodaDataTypeName; print([m.name for m in SodaDataTypeName])"` enumerates
the full set — your two required type-name dicts must round-trip every member
your engine can produce.

**Required overrides:**

| Method | Returns | Purpose |
|---|---|---|
| `get_data_source_data_type_name_by_soda_data_type_names()` | `dict[SodaDataTypeName, str]` | For DDL: how to spell each Soda type in this dialect |
| `get_soda_data_type_name_by_data_source_data_type_names()` | `dict[str, SodaDataTypeName]` | For schema checks: how to canonicalize what the DB returns |
| `get_database_prefix_index()` | `int \| None` | Index into `dataset_prefix` for database name (`None` if no concept) |
| `get_schema_prefix_index()` | `int \| None` | Index into `dataset_prefix` for schema name (`None` if flat namespace) |

Postgres uses `[database, schema]` (database=0, schema=1). DuckDB returns
`None` for database, `0` for schema.

**Frequently overridden:**

- `_build_regex_like_sql(matches: REGEX_LIKE) -> str` — every dialect spells
  this differently. Postgres: `expr ~ 'pattern'`. DuckDB:
  `REGEXP_MATCHES(expr, 'pattern')`. Snowflake: `REGEXP_LIKE(expr, 'pattern')`.
- `_build_cast_sql(cast: CAST) -> str` — Postgres uses `expr::type`, others
  use `CAST(expr AS type)` (the default).
- `create_schema_if_not_exists_sql(prefixes, add_semicolon)` — for engines
  with non-standard schema DDL.
- `escape_string(value)` / `escape_regex(value)` — quoting/escaping rules.
- `quote_default(identifier)` / `quote_for_ddl(identifier)` — identifier
  quoting (default: double-quote).
- `default_casify(identifier)` / `metadata_casify(identifier)` — case
  normalisation. Postgres lowercases unquoted identifiers; Snowflake uppercases.
- `_get_data_type_name_synonyms()` — returns `list[list[str]]` of equivalent
  native type names (e.g. `["varchar", "character varying"]`).
- `supports_data_type_character_maximum_length()`, `supports_data_type_numeric_precision()`,
  `supports_data_type_numeric_scale()`, `supports_data_type_datetime_precision()` —
  default `True`; override to `False` if the dialect doesn't expose that info.
- `default_numeric_precision()` / `default_numeric_scale()` — returned when
  the engine doesn't report explicit precision/scale.
- `supports_materialized_views()` — default `False`.
- `supports_sampler(SamplerType)` / `_build_sample_sql(...)` — for
  `TABLESAMPLE`-style sampling.

**Metadata queries** — defaults use `INFORMATION_SCHEMA`. If your engine
doesn't have one, override `build_columns_metadata_query_str(...)` and
`build_all_columns_metadata_query_str(...)`. Postgres builds queries against
`pg_catalog.pg_class` etc. via the `sql_ast` builder DSL — see lines 248-431
of `postgres_data_source.py` for a full example.

If the engine doesn't support bulk metadata queries (e.g. only `DESCRIBE TABLE`
per table), override `bulk_columns_metadata_available` on your `DataSourceImpl`
to return `False`.

### 2.5. `DataSourceImpl` subclass

```python
class {Name}DataSourceImpl(DataSourceImpl, model_class={Name}DataSource):
    def _create_sql_dialect(self) -> SqlDialect:
        return {Name}SqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return {Name}DataSourceConnection(
            name=self.data_source_model.name,
            connection_properties=self.data_source_model.connection_properties,
        )
```

The `model_class=` kwarg in the class declaration triggers
`__init_subclass__` registration in `DataSourceImpl`. Without it, your data
source will not be discoverable.

If your engine needs custom metadata queries, also override
`create_metadata_tables_query()` to return a subclass of `MetadataTablesQuery`
(see `PostgresMetadataTablesQuery`).

### 2.6. `pyproject.toml`

```toml
[project]
name = "soda-{name}"
version = "<MATCH SODA-CORE VERSION>"
description = "Soda {Name} V4"
requires-python = ">=3.10"
dependencies = [
    "soda-core==<MATCH>",
    "{your-dbapi-driver}>={X.Y}",
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

The entry-point group `soda.plugins.data_source.{name}` is what makes your
package discoverable. By convention the `{name}` suffix matches the
`Literal[...]` `type` field on your `DataSourceBase` subclass, but the loader
does **not** read the entry-point name to match — `ep.load()` is just an import
trigger that fires `DataSourceImpl.__init_subclass__(model_class=...)`, which
registers the impl class keyed by `model_class.get_class_type()` (i.e. the
`Literal[...]` default). The actual lookup at YAML parse time uses the `type:`
field in user YAML against that registry. See `DataSourceImpl.from_yaml_source`
and `__init_subclass__` in `data_source_impl.py`.

Keep the entry-point suffix, the `Literal[...]` value, and the `type:` field
in YAML aligned for sanity — but the load-bearing match is YAML-`type:` ↔
`Literal[...]`.

### 2.7. Workspace registration

Add to the repo root `pyproject.toml`:

- `[tool.uv.workspace] members` — add `"soda-{name}"`
- `[tool.uv.sources]` — add `soda-{name} = { workspace = true }`

Add to the repo root `pytest.ini`:

- `testpaths` — add `soda-{name}/tests`
- `pythonpath` — add `soda-{name}/tests`

### 2.8. Test helper

In `src/soda_{name}/test_helpers/{name}_data_source_test_helper.py`:

```python
from helpers.data_source_test_helper import DataSourceTestHelper

class {Name}DataSourceTestHelper(DataSourceTestHelper):
    def _create_database_name(self) -> Optional[str]:
        return os.getenv("{NAME}_DATABASE", "soda_test")

    def _create_data_source_yaml_str(self) -> str:
        return f"""
            type: {name}
            name: {self.name}
            connection:
                host: {os.getenv("{NAME}_HOST", "localhost")}
                user: {os.getenv("{NAME}_USERNAME", "soda_test")}
                password: {os.getenv("{NAME}_PASSWORD")}
                port: {int(os.getenv("{NAME}_PORT", "..."))}
                database: {self.dataset_prefix[0]}
        """
```

Then register in `soda-tests/src/helpers/data_source_test_helper.py`'s
`DataSourceTestHelper.create()` factory — add an `elif` branch for your
`{name}`.

If your data source has no schema concept, override `_create_schema_name()`
to return `None`. If it has unusual case rules, override `_adjust_schema_name()`.

### 2.9. Conftest

In `soda-{name}/tests/conftest.py`:

```python
from helpers.test_fixtures import *  # noqa: F401
from soda_core.common.logging_configuration import configure_logging

def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)
```

This wires in the standard fixtures (`data_source_test_helper`, etc.).

---

## 3. Adding a check type — step by step

### 3.1. Two `*_yaml.py` classes

`{type}_check_yaml.py`:

```python
class {Type}CheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["{type}"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        return {Type}CheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class {Type}CheckYaml(ThresholdCheckYaml):  # or CheckYaml / MissingAncValidityCheckYaml
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
        # read additional fields:
        # self.foo = check_yaml_object.read_string("foo")
        # self.bar = check_yaml_object.read_number_opt("bar")
```

Pick the right base:

- `CheckYaml` — bare check, no threshold (e.g. schema check). **Do not** use
  if you have a `threshold:` block — pick `ThresholdCheckYaml` instead.
- `ThresholdCheckYaml` — has a `threshold:` block (most metric-style checks).
  **Do not** use for checks that have no user-tunable threshold; you'll get
  spurious validation errors.
- `MissingAncValidityCheckYaml` — adds `missing_*` and `valid_*` fields.
  **Do not** use unless you actually parse those fields; otherwise pick
  `ThresholdCheckYaml`. (Yes — `Anc` is a typo carried forward from current
  code. The corresponding impl base is correctly spelled
  `MissingAndValidityCheckImpl`. The names will be unified in a separate PR.)

Use `check_yaml_object.read_string` / `read_string_opt` / `read_number_opt` /
`read_bool_opt` / `read_object_opt` for typed parsing with location-aware
errors. `read_*_opt` returns `None` if the key is absent;
non-`opt` variants log an error and return `None`.

### 3.2. Two `*_check.py` classes

```python
class {Type}CheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["{type}"]

    def parse_check(
        self, contract_impl: ContractImpl, column_impl: Optional[ColumnImpl], check_yaml: {Type}CheckYaml,
    ) -> Optional[CheckImpl]:
        return {Type}CheckImpl(contract_impl, column_impl, check_yaml)


class {Type}CheckImpl(CheckImpl):
    def __init__(self, contract_impl, column_impl, check_yaml):
        super().__init__(contract_impl=contract_impl, column_impl=column_impl, check_yaml=check_yaml)
        # threshold: only if you extended ThresholdCheckYaml
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
            default_threshold=ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be_greater_than=0),
        )

    def setup_metrics(self, contract_impl, column_impl, check_yaml) -> None:
        # called once per check; resolve metrics that should be measured
        self.{your}_metric = self._resolve_metric(
            {Your}MetricImpl(contract_impl=contract_impl, check_impl=self),
        )

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        value = measurement_values.get_value(self.{your}_metric)
        outcome = self.evaluate_threshold(value)  # or your custom logic
        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=value,
            diagnostic_metric_values={"...": value},
        )
```

### 3.3. Metric

For SQL aggregation (counted in the same query as the rest of the contract):

```python
class {Your}MetricImpl(AggregationMetricImpl):
    def __init__(self, contract_impl, check_impl=None, ...):
        super().__init__(
            contract_impl=contract_impl,
            metric_type="{your_metric}",
            check_filter=check_impl.check_yaml.filter if check_impl else None,
            missing_and_validity=None,
        )

    def sql_expression(self) -> SqlExpression:
        # use the sql_ast builders: SUM, COUNT, CASE_WHEN, COLUMN, LITERAL, ...
        return COUNT(STAR())

    def convert_db_value(self, value: any) -> any:
        return int(value) if value is not None else 0
```

For checks that need their own query (e.g. freshness picks `MAX(updated_at)`
in a separate query and timestamps it against `NOW()`), subclass `MetricImpl`
directly and emit the query via the contract's query builder. See
`freshness_check.py` for the pattern.

### 3.4. Register

Either:

**(a)** if you ship inside an existing package, add to that package's plugin's
`load()` classmethod:

```python
CheckYaml.register({Type}CheckYamlParser())
CheckImpl.register({Type}CheckParser())
```

See `CoreCheckTypesPlugin.register_check_types()` for the canonical pattern.

**(b)** if you ship in a new package, declare a `Plugin`:

```python
from soda_core.plugins import Plugin

class {Pkg}CheckTypesPlugin(Plugin):
    @classmethod
    def setup_cli(cls, root_parser): pass

    @classmethod
    def load(cls):
        from soda_core.contracts.impl.contract_yaml import CheckYaml
        from soda_core.contracts.impl.contract_verification_impl import CheckImpl
        CheckYaml.register({Type}CheckYamlParser())
        CheckImpl.register({Type}CheckParser())
        return cls()
```

Entry point:

```toml
[project.entry-points."soda.plugins.check_types.{pkg}"]
{Pkg}CheckTypesPlugin = "soda_{pkg}.check_types:{Pkg}CheckTypesPlugin"
```

The plugin loader (`soda_core.plugins.load_plugins`) discovers all entry
points under `soda.plugins.*`, so any group prefix works; the `check_types.`
segment is convention.

---

## 4. SQL-AST builders cheat sheet

Use these in `sql_expression()` and SQL-building dialect methods.
All importable from `soda_core.common.sql_ast`:

| Category | Builders |
|---|---|
| Selection | `SELECT`, `FROM`, `WHERE`, `AND`, `OR`, `NOT`, `JOIN`, `LEFT_INNER_JOIN`, `ORDER_BY_ASC`, `LIMIT`, `OFFSET` |
| Columns / values | `COLUMN`, `STAR`, `LITERAL`, `RAW_SQL`, `CAST` |
| Aggregation | `COUNT`, `SUM`, `AVERAGE`, `MIN`, `MAX`, `DISTINCT` |
| Conditions | `EQ`, `GT`, `LT`, `GTE`, `LTE`, `IN`, `IS_NULL`, `IS_NOT_NULL`, `LIKE`, `NOT_LIKE`, `REGEX_LIKE`, `EXISTS` |
| Functions | `LOWER`, `LENGTH`, `COALESCE`, `CASE_WHEN`, `CONCAT`, `CONCAT_WS` |
| DDL/DML | `CREATE_TABLE`, `CREATE_TABLE_COLUMN`, `CREATE_VIEW`, `CREATE_MATERIALIZED_VIEW`, `DROP_TABLE`, `INSERT_INTO`, `VALUES`, `VALUES_ROW` |
| Strings | `SqlExpressionStr` (raw string fallback when no builder fits) |

Render via the dialect's `build_select_sql(elements)` /
`build_expression_sql(expr)` / `build_create_table_sql(...)`. Don't hand-write
SQL strings unless absolutely necessary — they bypass dialect rules.

---

## 5. Plugin discovery — how it actually works

`soda_core.plugins.load_plugins()` is called from
`cli.py`'s entry point. It calls `entry_points()` and iterates every group
starting with `soda.plugins.`. For each entry, it `ep.load()`s the class. If
the class implements the `Plugin` Protocol (has `setup_cli` + `load`
classmethods), it gets called.

For data sources, the implementation class itself is **not** a `Plugin` — the
`isinstance(plugin_cls, Plugin)` check fails and `setup_cli`/`load` are
skipped. Registration happens via `DataSourceImpl.__init_subclass__` at
**import time**, triggered by `ep.load()` evaluating the entry-point string.
That's why your entry-point target must be the `*DataSourceImpl` class
itself, not a wrapper.

For check types, you **need** the `Plugin` class because the registration
calls (`CheckYaml.register(...)`, `CheckImpl.register(...)`) are imperative,
not metaclass-driven. Without a `load()` classmethod, the parsers never
register and your check type silently disappears.

---

## 6. Verification

Before claiming the work is done, all of these must pass:

```bash
# 1. Workspace install resolves
uv sync --all-packages --group dev

# 2. Pre-commit clean
uv run pre-commit run --all-files

# 3. Plugin discovery sees your data source
uv run python -c "
from soda_core.plugins import load_plugins
from importlib.metadata import entry_points
load_plugins()
print([ep.name for ep in entry_points(group='soda.plugins.data_source.{name}')])
"
# expected output: ['{Name}DataSourceImpl']

# 4. YAML parse + connect smoke test
TEST_DATASOURCE={name} uv run pytest soda-{name}/tests/data_sources/test_{name}.py -v

# 5. Cross-data-source suite (this is the bar)
TEST_DATASOURCE={name} uv run pytest soda-tests/tests -x

# 6. No leftover NotImplementedError
grep -rn NotImplementedError soda-{name}/src
```

Tests must pass, not be skipped. If you skip a test, document why in the test
itself with `pytest.skip("reason: ...")` and surface the list in your PR.

For check types, the equivalent verification is:

```bash
uv run python -c "
from soda_core.plugins import load_plugins; load_plugins()
from soda_core.contracts.impl.contract_verification_impl import CheckImpl
print('{type}' in CheckImpl.get_check_type_names())
"
# expected: True
```

Plus pytest covering the new check type running against at least one data
source.

---

## 7. Common failure modes

| Symptom | Likely cause |
|---|---|
| `Data source type 'X' not available. Make sure to install the required plugin` | Nothing registered the type. Causes, in rough order: (1) the package isn't installed at all (no entry point to load); (2) the entry point exists but `ep.load()` raised silently during import — run `python -c "import soda_{name}.common.data_sources.{name}_data_source"` to surface the real error; (3) `model_class=` was omitted from the `DataSourceImpl` subclass declaration, so `__init_subclass__` never fired and the registry stayed empty; (4) the `Literal[...]` default on `DataSourceBase` doesn't match the `type:` value in the user's YAML (typo on either side). The entry-point name is never read — don't chase it. |
| Schema check passes locally but fails in cross-DS suite | Type-name mappings are incomplete. Run a `SELECT * FROM information_schema.columns` against your test schema and ensure every type the suite creates round-trips through your two type dicts. |
| `Unknown check type 'X'` after registering | The package's `Plugin.load()` classmethod isn't being called — ensure your entry-point group starts with `soda.plugins.` and the class implements both `setup_cli` and `load` classmethods. |
| Regex / LIKE / sample tests fail with SQL syntax error | Override `_build_regex_like_sql`, `_build_like_sql`, or `_build_sample_sql`. Defaults are Postgres-flavoured. |
| Identifier case mismatches (e.g. `MY_TABLE` vs `my_table`) | Override `default_casify` and `metadata_casify`. Snowflake uppercases by default; most others lowercase. |
| Failing-rows queries return nothing | Override `escape_string` if your engine uses non-standard string escaping. |
| `Cannot determine schema name from prefixes` | `get_schema_prefix_index()` is wrong, or your `_create_dataset_prefix()` in the test helper returns the wrong list shape for this dialect. |
| Pre-commit fails with import errors | Missing `__init__.py` in a new directory, or import paths don't match the package structure. |

---

## 8. What you do **not** need to do

- Do not edit `soda-core/` itself unless you found a real upstream bug.
- Do not register your data source in any central registry (entry points are
  the only registration).
- Do not write SQL by string concatenation — use `sql_ast` builders.
- Do not subclass `DataSourceTestHelper` for things that aren't actually
  data-source-specific. The base class handles 95% of test fixture setup.
- Do not duplicate metric logic that already exists in
  `contracts/impl/check_types/` — extend or reuse.

---

## 9. Pointers to canonical examples

| You need to look at... | Read |
|---|---|
| Full-featured DS with custom metadata SQL | `soda-postgres/src/soda_postgres/common/data_sources/postgres_data_source.py` |
| Minimal DS with no schema concept | `soda-duckdb/src/soda_duckdb/common/data_sources/duckdb_data_source.py` |
| DS with multi-warehouse semantics | `soda-snowflake/`, `soda-databricks/` |
| Aggregation check (simplest) | `soda-core/src/soda_core/contracts/impl/check_types/row_count_check.py` |
| Check with own query | `soda-core/src/soda_core/contracts/impl/check_types/freshness_check.py` |
| Check with missing/validity semantics | `soda-core/src/soda_core/contracts/impl/check_types/missing_check.py` |
| Plugin registration of check types | `soda-core/src/soda_core/contracts/impl/check_types/check_types.py` |
| Test helper with credentials | `soda-postgres/src/soda_postgres/test_helpers/postgres_data_source_test_helper.py` |
| Cross-data-source test factory | `soda-tests/src/helpers/data_source_test_helper.py` line 65 onward |
