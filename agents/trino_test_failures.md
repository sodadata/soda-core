# Trino Test Failure Analysis

**Test run:** `TEST_DATASOURCE=trino uv run pytest soda-tests`
**Results (with `TRINO_CATALOG=iceberg`):** 8 failed, 300 passed, 6 skipped

## Key insight: failures are connector-dependent

Trino is a query engine that sits in front of many different connectors (Iceberg, PostgreSQL, Hive, Delta Lake, etc.). Each connector has different capabilities and type-handling behaviors. The `TRINO_CATALOG` env var selects which connector the tests run against:

- **`catalog=iceberg`** — Iceberg connector (S3/Glue backend). Used in CI and remote dev.
- **`catalog=db`** — PostgreSQL connector. Used for local development.

Since customers can use any connector, **fixes must not assume Iceberg-specific behavior applies to all of Trino**.

### Cross-catalog test results

| Test | catalog=db (postgres) | catalog=iceberg |
|------|----------------------|-----------------|
| `test_table_metadata` | PASSED | FAILED (varchar length lost) |
| `test_view_metadata` | FAILED (views not supported by connector) | FAILED (varchar length lost) |
| `test_mapping_canonical_to_data_type_to_canonical` | PASSED | FAILED (char→varchar, smallint→integer) |
| `test_column_expression` (x3) | FAILED (TYPE_MISMATCH on INSERT) | FAILED (TABLE_ALREADY_EXISTS) |

---

## Failure Group 1: Materialized views not discoverable via `information_schema.tables` -- FIXED

### Affected tests
- `soda-tests/tests/integration/test_schema_check.py::test_schema[TableType.MATERIALIZED_VIEW]`
- `soda-tests/tests/integration/test_table_metadata.py::test_materialized_view_detected_by_table_metadata`

### Error
```
trino.exceptions.TrinoUserError: TrinoUserError(type=USER_ERROR, name=ALREADY_EXISTS,
  message="Materialized view already exists: dev_paulteehan.sodatest_..._mv")
```

### Root cause
The test helper method `create_materialized_view_from_test_table` (in `data_source_test_helper.py:574`) checks for existing MVs before creating one by calling `query_existing_test_materialized_view_names()`. This queries `information_schema.tables` and filters for rows where `convert_table_type_to_enum(table_type)` returns `TableType.MATERIALIZED_VIEW`.

However, **Trino's `information_schema.tables` does not list materialized views**. Trino tracks MVs in the separate `system.metadata.materialized_views` table instead. An attempt to add MV differentiation to `information_schema.tables` ([trinodb/trino#8207](https://github.com/trinodb/trino/issues/8207)) was implemented in [PR #19947](https://github.com/trinodb/trino/pull/19947) but then **reverted** in [PR #20107](https://github.com/trinodb/trino/pull/20107) because adding non-standard columns to `information_schema` violates SQL specification compatibility. The official recommendation is to use `system.metadata.materialized_views` for MV metadata ([System connector docs](https://trino.io/docs/current/connector/system.html)).

As a result, `query_existing_test_materialized_view_names()` always returns an empty list, even when MVs exist from a prior test run. The code skips the `DROP MATERIALIZED VIEW` step and goes straight to `CREATE MATERIALIZED VIEW`, which fails with `ALREADY_EXISTS`.

The same issue causes `test_materialized_view_detected_by_table_metadata` to fail: the `MetadataTablesQuery` can never find MVs via `information_schema.tables` for Trino, so the assertion that the MV is found in the results fails.

### Fix applied
Created `TrinoMetadataTablesQuery` (in `soda-trino/src/soda_trino/statements/trino_metadata_tables_query.py`) that subclasses `MetadataTablesQuery`. When `types_to_return` includes `MATERIALIZED_VIEW`, it queries `system.metadata.materialized_views` instead of relying on `information_schema.tables`. For TABLE and VIEW types, it delegates to the standard base implementation. `TrinoDataSourceImpl.create_metadata_tables_query()` now returns this Trino-specific subclass.

---

## Failure Group 2: Iceberg connector drops type-specific metadata

### Affected tests
- `soda-tests/tests/integration/test_table_metadata.py::test_table_metadata`
- `soda-tests/tests/integration/test_table_metadata.py::test_view_metadata`
- `soda-tests/tests/integration/test_soda_data_types.py::test_mapping_canonical_to_data_type_to_canonical`

### Errors

**test_table_metadata / test_view_metadata (Iceberg only):**
```
AssertionError: assert False
  where False = is_same_data_type_for_schema_check(
    expected=SqlDataType(name='varchar', character_maximum_length=255, ...),
    actual=TrinoSqlDataType(name='varchar', character_maximum_length=None, ...))
```

**test_view_metadata (db only — different error):**
```
trino.exceptions.TrinoUserError: TrinoUserError(type=USER_ERROR, name=NOT_SUPPORTED,
  message="This connector does not support creating views")
```

**test_mapping_canonical_to_data_type_to_canonical (Iceberg only):**
```
AssertionError: assert False
  Column char_default -> Expected: SodaDataTypeName.CHAR, Actual: SodaDataTypeName.VARCHAR
```

### Root cause

These failures are **Iceberg-connector-specific**. The `db` (PostgreSQL) connector preserves type information correctly — `test_table_metadata` and `test_mapping_canonical_to_data_type_to_canonical` both pass with `catalog=db`.

Trino's Iceberg connector silently converts or drops certain type information:

| Created as | Reported by `information_schema.columns` | Impact |
|---|---|---|
| `char` / `char(100)` | `varchar` | CHAR becomes VARCHAR |
| `varchar(255)` | `varchar` | Length parameter is dropped |
| `smallint` | `integer` | SMALLINT becomes INTEGER |

This is inherent to the Iceberg table format — Iceberg's type system does not have `char` or `smallint`, and `varchar` is always unbounded.

**test_view_metadata** additionally fails with `catalog=db` because the PostgreSQL connector does not support creating views (`NOT_SUPPORTED` error). This is a separate connector limitation unrelated to type metadata.

### Why the original proposed fix was wrong

The original proposal to set `supports_data_type_character_maximum_length()` to `False` for all of `TrinoSqlDialect` would **break the `db` connector**, which correctly preserves varchar lengths. Similarly, adding `(CHAR, VARCHAR)` and `(SMALLINT, INTEGER)` to `SODA_DATA_TYPE_SYNONYMS` at the Trino dialect level would mask real type distinctions that the `db` connector correctly maintains.

Since customers can use any Trino connector, we cannot assume Iceberg's limitations apply globally.

### Proposed fix (revised)

These failures reflect real connector-specific behavior that varies across Trino connectors. Options:

1. **Make type capability methods connector-aware.** The `TrinoSqlDialect` could query the connector type at runtime (via `SHOW CATALOGS` or inspecting the catalog config) and return connector-appropriate values from `supports_data_type_character_maximum_length()`, `SODA_DATA_TYPE_SYNONYMS`, etc. This is the most correct approach but requires runtime introspection.

2. **Accept the Iceberg limitations as known and skip these tests for Iceberg.** Add a test marker or fixture that detects the connector type and skips tests that rely on type metadata fidelity when running against connectors known to drop it. This is pragmatic for CI.

3. **Use a "loose" comparison mode for schema checks.** When verifying contracts against an Iceberg-backed Trino catalog, schema checks could treat missing length/precision parameters as acceptable when the base type name matches. This protects customers from false positives when their Iceberg catalog doesn't preserve `varchar(255)` → only reports `varchar`.

Option 3 is the most customer-friendly: a user defining `varchar(255)` in a contract against an Iceberg-backed Trino catalog should not get a schema check failure just because Iceberg doesn't preserve the length constraint. However, this needs careful design to avoid suppressing real mismatches.

For **test_view_metadata with `catalog=db`**: The PostgreSQL connector doesn't support views. The existing `supports_views()` check in `TrinoSqlDialect` currently returns `True` unconditionally. This should ideally be connector-aware, or the test should handle the `NOT_SUPPORTED` error gracefully.

---

## Failure Group 3: Test table setup failures (connector-dependent)

### Affected tests
- `soda-tests/tests/feature/test_column_expression.py::test_column_level_column_expression_metric_checks_fail`
- `soda-tests/tests/feature/test_column_expression.py::test_check_level_column_expression_metric_checks_fail`
- `soda-tests/tests/feature/test_column_expression.py::test_column_expression_clashing_metric`

### Errors vary by connector

**With `catalog=iceberg` — TABLE_ALREADY_EXISTS:**
```
trino.exceptions.TrinoUserError: TrinoUserError(type=USER_ERROR, name=TABLE_ALREADY_EXISTS,
  message="line 1:1: Table 'iceberg.dev_paulteehan.sodatest_column_expression_d39293a2' already exists")
```

**With `catalog=db` — TYPE_MISMATCH:**
```
trino.exceptions.TrinoUserError: TrinoUserError(type=USER_ERROR, name=TYPE_MISMATCH,
  message="line 1:1: Insert query has mismatched column types: Table: [integer, integer, integer],
  Query: [integer, integer, varchar(2)]")
```

### Root cause

There are **two independent bugs** affecting the same tests on different connectors:

#### Bug A: Test table spec has wrong column type (affects `catalog=db`)

The test table specification in `test_column_expression.py:6` defines:
```python
test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("column_expression")
    .column_integer("id")
    .column_integer("age")
    .column_integer("age_str")    # ← defined as integer
    .rows(rows=[
        (1, 10, "10"),            # ← but data has string values
        (2, 20, "20"),
        ...
    ])
    .build()
)
```

The `age_str` column is defined as `integer` but the test data contains string values (`"10"`, `"20"`, etc.). The Iceberg connector performs implicit casting (varchar→integer), so the INSERT succeeds there. The PostgreSQL connector is strict about types and rejects the INSERT with `TYPE_MISMATCH`.

This is a **test definition bug**: the test uses `column_expression: '"age_str"::integer'` in the contract, which implies `age_str` should be a string column that gets cast to integer. The column should likely be defined as `column_varchar("age_str")` instead of `column_integer("age_str")`.

#### Bug B: Iceberg eventual consistency after DROP TABLE (affects `catalog=iceberg`)

The test table exists from a prior run but contains 0 rows (likely because Bug A's INSERT previously failed on Iceberg too, or rows were cleaned up). The `ensure_test_table` method detects the row count mismatch and enters the drop-and-recreate path:

1. **DROP TABLE** executes without error
2. **CREATE TABLE** fails with `TABLE_ALREADY_EXISTS`

The Iceberg connector has not fully removed the table from its metadata by the time the `CREATE TABLE` executes. This is a known Iceberg eventual-consistency behavior.

Note: The 0-row state was likely caused by Bug A manifesting differently in a previous test run. Once the table exists with 0 rows, subsequent runs hit Bug B.

### Proposed fix (revised)

Both bugs should be fixed:

1. **Fix the test table spec** — Change `column_integer("age_str")` to `column_varchar("age_str")` (or whichever type the test actually intends). This fixes the `catalog=db` TYPE_MISMATCH and prevents the 0-row table state that triggers Bug B.

2. **Add Iceberg resilience in the test helper** — Override `_create_and_insert_test_table` in `TrinoDataSourceTestHelper` to handle the Iceberg timing issue as a safety net. Even after fixing the test spec, the eventual-consistency race can occur whenever a table needs to be recreated:
   ```python
   def _create_and_insert_test_table(self, test_table: TestTable) -> None:
       # Iceberg may not fully clean up metadata after DROP TABLE,
       # so issue an extra DROP TABLE IF EXISTS as a safety net.
       drop_sql = f'DROP TABLE IF EXISTS {test_table.qualified_name}'
       self.data_source_impl.execute_update(drop_sql)
       super()._create_and_insert_test_table(test_table)
   ```
