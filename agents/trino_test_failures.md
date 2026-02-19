# Trino Test Failure Analysis

**Test run:** `TEST_DATASOURCE=trino uv run pytest soda-tests`
**Results:** 8 failed, 300 passed, 6 skipped

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

**test_table_metadata / test_view_metadata:**
```
AssertionError: assert False
  where False = is_same_data_type_for_schema_check(
    expected=SqlDataType(name='varchar', character_maximum_length=255, ...),
    actual=TrinoSqlDataType(name='varchar', character_maximum_length=None, ...))
```

**test_mapping_canonical_to_data_type_to_canonical:**
```
AssertionError: assert False
  Column char_default -> Expected: SodaDataTypeName.CHAR, Actual: SodaDataTypeName.VARCHAR
```

### Root cause
Trino's Iceberg connector silently converts or drops certain type information:

| Created as | Reported by `information_schema.columns` | Impact |
|---|---|---|
| `char` / `char(100)` | `varchar` | CHAR becomes VARCHAR |
| `varchar(255)` | `varchar` | Length parameter is dropped |
| `smallint` | `integer` | SMALLINT becomes INTEGER |

This causes two distinct failures:

**test_table_metadata / test_view_metadata:** The test creates a column `varchar_w_length varchar(255)`. `TrinoSqlDialect.supports_data_type_character_maximum_length()` returns `True`, so the test expects `character_maximum_length=255`. But Iceberg stores all varchars as unbounded, so `information_schema.columns` reports `data_type='varchar'` (no length), and `extract_character_maximum_length` correctly parses `None` from `'varchar'`. Expected=255, actual=None.

**test_mapping_canonical_to_data_type_to_canonical:** The test expects `char_default` to round-trip as `SodaDataTypeName.CHAR`. But Iceberg stores `char` as `varchar`, so the reverse mapping (`get_soda_data_type_name_by_data_source_data_type_names`) returns `VARCHAR`. The `SODA_DATA_TYPE_SYNONYMS` for Trino only includes `(TEXT, VARCHAR)` and `(NUMERIC, DECIMAL)` — it's missing `(CHAR, VARCHAR)` and `(SMALLINT, INTEGER)`.

### Proposed fix

1. **`supports_data_type_character_maximum_length()`** should return `False` in `TrinoSqlDialect` (at `trino_data_source.py:158`). The Iceberg connector does not preserve varchar/char length constraints, so Soda should not expect or compare them. This fixes test_table_metadata and test_view_metadata.

2. **Add synonym pairs** to `TrinoSqlDialect.SODA_DATA_TYPE_SYNONYMS` (at `trino_data_source.py:52`):
   ```python
   SODA_DATA_TYPE_SYNONYMS = (
       (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR),
       (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
       (SodaDataTypeName.CHAR, SodaDataTypeName.VARCHAR),      # Iceberg stores char as varchar
       (SodaDataTypeName.SMALLINT, SodaDataTypeName.INTEGER),   # Iceberg stores smallint as integer
   )
   ```
   This fixes test_mapping_canonical_to_data_type_to_canonical.

---

## Failure Group 3: Test table recreation fails (`TABLE_ALREADY_EXISTS`)

### Affected tests
- `soda-tests/tests/feature/test_column_expression.py::test_column_level_column_expression_metric_checks_fail`
- `soda-tests/tests/feature/test_column_expression.py::test_check_level_column_expression_metric_checks_fail`
- `soda-tests/tests/feature/test_column_expression.py::test_column_expression_clashing_metric`

### Error
```
trino.exceptions.TrinoUserError: TrinoUserError(type=USER_ERROR, name=TABLE_ALREADY_EXISTS,
  message="line 1:1: Table 'iceberg.dev_paulteehan.sodatest_column_expression_d39293a2' already exists")
```

### Root cause
The test table `SODATEST_column_expression_d39293a2` exists from a prior test run but contains 0 rows. The `ensure_test_table` method in `data_source_test_helper.py:355` detects the row count mismatch (0 vs expected 5) and enters the drop-and-recreate path:

1. **DROP TABLE** `"iceberg"."dev_paulteehan"."sodatest_column_expression_d39293a2"` — executes without error
2. **CREATE TABLE** `"iceberg"."dev_paulteehan"."SODATEST_column_expression_d39293a2"` — fails with `TABLE_ALREADY_EXISTS`

The `DROP TABLE` appears to succeed (no exception raised), but the subsequent `CREATE TABLE` fails because the Iceberg connector has not fully removed the table from its metadata by the time the `CREATE TABLE` executes. This is a known behavior pattern with Iceberg table operations where metadata cleanup is eventually consistent.

Note: Trino lowercases all identifiers (even quoted ones for the Iceberg connector), so the uppercase `SODATEST_...` and lowercase `sodatest_...` refer to the same table.

All 3 feature tests share the same `test_table_specification`, and they share a session-scoped `data_source_test_helper`. Once the first test fails during table setup, all subsequent tests in the file also fail.

### Proposed fix

Override `_create_and_insert_test_table` (or `_drop_test_table`) in `TrinoDataSourceTestHelper` to handle the Iceberg timing issue. Options:

1. **Use `DROP TABLE IF EXISTS` immediately before `CREATE TABLE`** by overriding `_create_test_table` in the Trino test helper to issue `DROP TABLE IF EXISTS` as a safety net right before creation.
2. **Override to use `CREATE TABLE IF NOT EXISTS`** followed by a `DELETE FROM` and `INSERT INTO`, avoiding the race entirely.
3. **Add retry logic** in the Trino test helper: if `CREATE TABLE` fails with `TABLE_ALREADY_EXISTS`, wait briefly and retry the drop+create sequence.

Option 1 is the simplest. The override in `TrinoDataSourceTestHelper` would look like:
```python
def _create_and_insert_test_table(self, test_table: TestTable) -> None:
    # Iceberg may not fully clean up metadata after DROP TABLE,
    # so issue an extra DROP TABLE IF EXISTS as a safety net.
    drop_sql = f'DROP TABLE IF EXISTS {test_table.qualified_name}'
    self.data_source_impl.execute_update(drop_sql)
    super()._create_and_insert_test_table(test_table)
```
