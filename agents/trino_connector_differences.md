# Trino Connector Differences: Runtime-Queryable Distinctions

**Date:** 2026-02-19
**Instance:** `trino.db.dev.sodadata.io`
**Catalogs tested:** `db` (PostgreSQL connector), `iceberg` (Iceberg connector)

---

## 1. How to identify the connector type at runtime

There is no `system.metadata.catalog_properties` table (doesn't exist in this Trino version), and `SHOW CREATE SCHEMA` does not reveal the connector name. However, several indirect methods reliably distinguish connectors:

### Method A: Session properties (most reliable)

```sql
SHOW SESSION LIKE '{catalog_name}.%'
```

Each connector exposes a unique set of session properties prefixed with the catalog name. The **property names themselves** are connector-specific:

| Signal | db (PostgreSQL) | iceberg |
|--------|----------------|---------|
| `{catalog}.array_mapping` | present | absent |
| `{catalog}.write_batch_size` | present | absent |
| `{catalog}.expire_snapshots_min_retention` | absent | present |
| `{catalog}.merge_manifests_on_write` | absent | present |
| `{catalog}.parquet_*` properties | absent | present (many) |
| `{catalog}.orc_*` properties | absent | present (many) |
| Total session properties | 20 | 45 |

**Detection strategy:** Query session properties and look for connector-fingerprint properties. For example, `{catalog}.expire_snapshots_min_retention` is exclusive to Iceberg; `{catalog}.array_mapping` is exclusive to PostgreSQL.

### Method B: `system.metadata.table_properties`

```sql
SELECT COUNT(*) FROM system.metadata.table_properties WHERE catalog_name = '{catalog_name}'
```

| Catalog | table_properties count | schema_properties count | mv_properties count |
|---------|----------------------|------------------------|---------------------|
| db      | 0                    | 0                      | 0                   |
| iceberg | 13                   | 1                      | 14                  |

The PostgreSQL connector exposes **zero** metadata properties across all three system tables. The Iceberg connector exposes many. A catalog with `table_properties_count > 0` that includes properties like `format`, `format_version`, or `partitioning` is almost certainly Iceberg.

### Method C: Feature probing (try-and-catch)

```sql
CREATE VIEW {catalog}.{schema}.probe_view AS SELECT 1 AS x
-- If NOT_SUPPORTED error → connector doesn't support views
```

| Feature | db (PostgreSQL) | iceberg |
|---------|----------------|---------|
| `CREATE VIEW` | NOT_SUPPORTED | supported |
| `CREATE MATERIALIZED VIEW` | NOT_SUPPORTED | supported |

---

## 2. Type behavior differences

Created the same table with identical DDL on both connectors and read back from `information_schema.columns`:

```sql
CREATE TABLE {catalog}.{schema}.soda_type_probe (
    col_varchar_255  varchar(255),
    col_varchar      varchar,
    col_char_10      char(10),
    col_char         char,
    col_smallint     smallint,
    col_integer      integer,
    col_bigint       bigint,
    col_decimal_10_2 decimal(10,2),
    col_boolean      boolean,
    col_date         date,
    col_timestamp    timestamp,
    col_timestamp_tz timestamp with time zone,
    col_double       double,
    col_real         real
)
```

### Side-by-side results

| Column | db (PostgreSQL) | iceberg | Match? |
|--------|----------------|---------|--------|
| `col_varchar_255` | `varchar(255)` | `varchar` | **NO** — Iceberg drops length |
| `col_varchar` | `varchar` | `varchar` | yes |
| `col_char_10` | `char(10)` | `varchar` | **NO** — Iceberg converts char→varchar |
| `col_char` | `char(1)` | `varchar` | **NO** — Iceberg converts char→varchar |
| `col_smallint` | `smallint` | `integer` | **NO** — Iceberg promotes to integer |
| `col_integer` | `integer` | `integer` | yes |
| `col_bigint` | `bigint` | `bigint` | yes |
| `col_decimal_10_2` | `decimal(10,2)` | `decimal(10,2)` | yes |
| `col_boolean` | `boolean` | `boolean` | yes |
| `col_date` | `date` | `date` | yes |
| `col_timestamp` | `timestamp(3)` | `timestamp(6)` | **NO** — different default precision |
| `col_timestamp_tz` | `timestamp(3) with time zone` | `timestamp(6) with time zone` | **NO** — different default precision |
| `col_double` | `double` | `double` | yes |
| `col_real` | `real` | `real` | yes |

### Summary of Iceberg-specific type coercions

| Behavior | PostgreSQL connector | Iceberg connector |
|----------|---------------------|-------------------|
| `char` type | Preserved as `char` | Converted to `varchar` |
| `varchar(N)` length | Preserved | Dropped (unbounded `varchar`) |
| `smallint` type | Preserved | Promoted to `integer` |
| Timestamp default precision | 3 (milliseconds) | 6 (microseconds) |

---

## 3. Feature support differences

| Feature | db (PostgreSQL) | iceberg |
|---------|----------------|---------|
| Views (`CREATE VIEW`) | not supported | supported |
| Materialized views (`CREATE MATERIALIZED VIEW`) | not supported | supported |
| Table properties (`system.metadata.table_properties`) | none | 13 properties |
| Schema properties (`system.metadata.schema_properties`) | none | 1 property |
| MV properties (`system.metadata.materialized_view_properties`) | none | 14 properties |
| Implicit type casting on INSERT (`varchar` → `integer`) | not supported (strict) | supported |

---

## 4. Session properties as connector fingerprints

The full set of session properties per connector serves as a reliable fingerprint.

### db (PostgreSQL connector) — 20 session properties
```
db.aggregation_pushdown_enabled
db.array_mapping                          ← PostgreSQL-specific
db.complex_join_pushdown_enabled
db.decimal_default_scale
db.decimal_mapping
db.decimal_rounding_mode
db.domain_compaction_threshold
db.dynamic_filtering_enabled
db.dynamic_filtering_wait_timeout
db.enable_string_pushdown_with_collate
db.join_pushdown_automatic_max_join_to_tables_ratio
db.join_pushdown_automatic_max_table_size
db.join_pushdown_enabled
db.join_pushdown_strategy
db.non_transactional_insert
db.non_transactional_merge
db.topn_pushdown_enabled
db.unsupported_type_handling
db.write_batch_size                       ← PostgreSQL-specific
db.write_parallelism
```

### iceberg — 45 session properties
```
iceberg.bucket_execution_enabled
iceberg.collect_extended_statistics_on_write
iceberg.dynamic_filtering_wait_timeout
iceberg.expire_snapshots_min_retention     ← Iceberg-specific
iceberg.extended_statistics_enabled
iceberg.file_based_conflict_detection_enabled
iceberg.idle_writer_min_file_size
iceberg.incremental_refresh_enabled
iceberg.max_partitions_per_writer
iceberg.merge_manifests_on_write           ← Iceberg-specific
iceberg.minimum_assigned_split_weight
iceberg.orc_bloom_filters_enabled          ← file-format (Iceberg-specific)
iceberg.orc_lazy_read_small_ranges
iceberg.orc_max_buffer_size
iceberg.orc_max_merge_distance
iceberg.orc_max_read_block_size
iceberg.orc_nested_lazy_enabled
iceberg.orc_stream_buffer_size
iceberg.orc_string_statistics_limit
iceberg.orc_tiny_stripe_threshold
iceberg.orc_writer_max_dictionary_memory
iceberg.orc_writer_max_row_group_rows
iceberg.orc_writer_max_stripe_rows
iceberg.orc_writer_max_stripe_size
iceberg.orc_writer_min_stripe_size
iceberg.orc_writer_validate_mode
iceberg.orc_writer_validate_percentage
iceberg.parquet_ignore_statistics          ← file-format (Iceberg-specific)
iceberg.parquet_max_read_block_row_count
iceberg.parquet_max_read_block_size
iceberg.parquet_small_file_threshold
iceberg.parquet_use_bloom_filter
iceberg.parquet_vectorized_decoding_enabled
iceberg.parquet_writer_batch_size
iceberg.parquet_writer_block_size
iceberg.parquet_writer_page_size
iceberg.parquet_writer_page_value_count
iceberg.projection_pushdown_enabled
iceberg.query_partition_filter_required
iceberg.query_partition_filter_required_schemas
iceberg.remove_orphan_files_min_retention
iceberg.sorted_writing_enabled
iceberg.statistics_enabled
iceberg.target_max_file_size
iceberg.use_file_size_from_metadata
```

---

## 5. Design constraints for Soda

Any approach must respect these constraints:

1. **Unknown connectors**: Trino supports dozens of connectors (Iceberg, Hive, Delta Lake, PostgreSQL, MySQL, Oracle, SQL Server, Kudu, Pinot, Raptor, Phoenix, Accumulo, Druid, Kafka, Kinesis, Redis, MongoDB, Elasticsearch, ...). We can't enumerate them all, and new ones appear over time. The design must handle connectors we've never tested.

2. **Read-only in production**: Soda does not have (and should not require) write access to customer databases. We cannot probe capabilities by creating/dropping test objects.

3. **Capabilities change over time**: A connector that drops varchar lengths today may preserve them in a future Trino release. Hardcoding connector-specific behavior is fragile.

---

## 6. Read-only information available at runtime

These queries are read-only and always available:

### A. Session properties (read-only, per-catalog)

```sql
SHOW SESSION LIKE '{catalog_name}.%'
```

Returns connector-specific configuration knobs. Useful for broad categorization (file-format-based vs RDBMS-based) but the specific property names vary by connector and Trino version, so building an exhaustive fingerprint table is not viable.

### B. `system.metadata.table_properties` (read-only)

```sql
SELECT property_name FROM system.metadata.table_properties WHERE catalog_name = '{catalog_name}'
```

| Catalog type | Properties available |
|-------------|---------------------|
| RDBMS connectors (e.g. PostgreSQL) | 0 (no table properties exposed) |
| File-format connectors (e.g. Iceberg) | Many (`format`, `partitioning`, `location`, etc.) |

This is a reliable **category-level** signal: "file-format-based storage" vs "pass-through to an RDBMS". But it doesn't tell you exactly *which* types get coerced or how.

### C. `information_schema.columns` on the user's real table (read-only)

```sql
SELECT column_name, data_type FROM {catalog}.information_schema.columns
WHERE table_schema = '{schema}' AND table_name = '{table}'
```

This is the most important signal because it's **ground truth**: it shows exactly what the connector reports for the actual table Soda is checking. No probing needed — during a contract verification, Soda already reads this.

---

## 7. Recommended approach: observe, don't identify

Rather than trying to identify the connector and then look up its known limitations, the schema check comparison should **work with what the connector actually reports**.

### The core problem

When a user writes a contract specifying `varchar(255)` and the connector reports `varchar`, the current `is_same_data_type_for_schema_check` comparison fails because `character_maximum_length` is `255` vs `None`. But this isn't a real schema violation — it's a connector limitation. The column is still a varchar.

### Proposed comparison logic

Schema checks should compare in layers, from most to least important:

1. **Base type family match** (mandatory): Does the canonical Soda data type match? `varchar(255)` → `VARCHAR`, `varchar` → `VARCHAR`. Both are `VARCHAR` → base type matches.

2. **Type parameter match** (when available): Only compare length/precision/scale when **both** the contract and the connector report a value. If the connector reports `varchar` with no length (i.e., `character_maximum_length=None`), don't fail — the connector simply doesn't track that parameter. If both report a length and they differ, that's a real mismatch.

3. **Type promotion tolerance**: When the connector reports a wider type than the contract specifies (e.g., contract says `smallint`, connector reports `integer`), this is a known pattern in file-format storage. This could be:
   - A configurable warning vs. failure
   - Handled via `SODA_DATA_TYPE_SYNONYMS` — but these should be **general** pairs that are safe across all connectors, not connector-specific

### What this means for the current code

The key change is in how `is_same_data_type_for_schema_check` handles missing parameters:

```
Contract:  varchar(255) → SqlDataType(name='varchar', character_maximum_length=255)
Connector: varchar      → SqlDataType(name='varchar', character_maximum_length=None)

Current behavior:  255 != None → FAIL
Proposed behavior: base type 'varchar' matches, connector doesn't report length → PASS
```

This works for **any** connector — if it preserves lengths, both sides have values and they get compared. If it doesn't, the connector side is `None` and we skip the parameter check. No connector identification needed.

### For type promotions (char→varchar, smallint→integer)

These are trickier because the base type name itself differs. Options:

- **Broad synonyms**: Add `(CHAR, VARCHAR)` and `(SMALLINT, INTEGER)` to `SODA_DATA_TYPE_SYNONYMS` for Trino globally. These are safe because even on connectors that preserve the distinction, `char` IS a kind of `varchar` and `smallint` IS a kind of `integer`. A user who specifies `char` in their contract won't get a false failure if the connector promotes it.

- **Directional tolerance**: Allow the connector to report a *wider* type than the contract specifies (smallint→integer is fine, integer→smallint would fail). This is more nuanced but requires defining a type hierarchy.

### For tests specifically

Tests DO have write access, so the test helper can additionally:

- Use `system.metadata.table_properties` to detect file-format connectors and skip or adapt type-sensitive tests
- Override drop/create behavior with safety nets (e.g., `DROP TABLE IF EXISTS` before create)
- Fix the `test_column_expression` table spec where `age_str` is mistyped as `integer`
