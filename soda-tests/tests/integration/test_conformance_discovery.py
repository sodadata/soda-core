"""
Adapter Conformance Tests: Metadata Discovery

Validates that every adapter's metadata discovery correctly:
- Filters out internal/temporary objects
- Returns accurate column type information through a full round-trip
- Maps type synonyms bidirectionally
- Reports column type parameters (precision, scale, length)

This is the #2 source of field bugs (~20% of historical fixes).

See: projects/enhancements/common_bugs_tests/historical-bug-analysis.md
"""

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.metadata_types import ColumnMetadata, SodaDataTypeName
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.table_types import (
    FullyQualifiedTableName,
    FullyQualifiedViewName,
)

# ---------------------------------------------------------------------------
# Test tables
# ---------------------------------------------------------------------------

# A table with all Soda data types to exercise the full type mapping round-trip.
all_types_table = (
    TestTableSpecification.builder()
    .table_purpose("conf_discovery_types")
    .column_varchar("col_varchar")
    .column_text("col_text")
    .column_integer("col_integer")
    .column_bigint("col_bigint")
    .column_smallint("col_smallint")
    .column_float("col_float")
    .column_double("col_double")
    .column_boolean("col_boolean")
    .column_date("col_date")
    .column_timestamp("col_timestamp")
    .column_timestamp_tz("col_timestamp_tz")
    .column_numeric("col_numeric")
    .column_decimal("col_decimal")
    .column_char("col_char")
    .column_time("col_time")
    .build()
)

# A table with specific type parameters to test precision/scale/length discovery.
typed_params_table = (
    TestTableSpecification.builder()
    .table_purpose("conf_discovery_params")
    .column_varchar("varchar_100", character_maximum_length=100)
    .column_char("char_10", character_maximum_length=10)
    .column_numeric("numeric_18_4", numeric_precision=18, numeric_scale=4)
    .column_decimal("decimal_10_2", numeric_precision=10, numeric_scale=2)
    .column_timestamp("ts_precision_3", datetime_precision=3)
    .column_timestamp_tz("ts_tz_precision_6", datetime_precision=6)
    .build()
)

# Simple table for discovery filtering tests.
simple_table = (
    TestTableSpecification.builder()
    .table_purpose("conf_discovery_filter")
    .column_varchar("name")
    .column_integer("value")
    .rows(
        [
            ("alpha", 1),
            ("bravo", 2),
        ]
    )
    .build()
)


# ---------------------------------------------------------------------------
# Internal object filtering
# ---------------------------------------------------------------------------


def test_discovery_excludes_soda_internal_tables(data_source_test_helper: DataSourceTestHelper):
    """Metadata discovery must not return __soda_temp* or other internal tables.

    Historical bug: commit a16b99c8 — __soda_temp tables were appearing in discovery results.
    """
    test_table = data_source_test_helper.ensure_test_table(simple_table)

    metadata_query = data_source_test_helper.data_source_impl.create_metadata_tables_query()
    results = metadata_query.execute(
        database_name=data_source_test_helper.extract_database_from_prefix(),
        schema_name=data_source_test_helper.extract_schema_from_prefix(),
    )

    internal_tables = []
    for entry in results:
        name = None
        if isinstance(entry, FullyQualifiedTableName):
            name = entry.table_name
        elif isinstance(entry, FullyQualifiedViewName):
            name = entry.view_name
        if name and name.lower().startswith("__soda"):
            internal_tables.append(name)

    assert internal_tables == [], f"Internal Soda tables leaked into discovery results: {internal_tables}"


def test_discovery_finds_test_table(data_source_test_helper: DataSourceTestHelper):
    """Verify that a newly created table IS discoverable via metadata query."""
    test_table = data_source_test_helper.ensure_test_table(simple_table)

    metadata_query = data_source_test_helper.data_source_impl.create_metadata_tables_query()
    results = metadata_query.execute(
        database_name=data_source_test_helper.extract_database_from_prefix(),
        schema_name=data_source_test_helper.extract_schema_from_prefix(),
        include_table_name_like_filters=[f"{test_table.unique_name}"],
    )

    table_names = [entry.table_name.lower() for entry in results if isinstance(entry, FullyQualifiedTableName)]
    assert (
        test_table.unique_name.lower() in table_names
    ), f"Test table {test_table.unique_name} not found in discovery. Found: {table_names}"


# ---------------------------------------------------------------------------
# View discovery via contract
# ---------------------------------------------------------------------------


def test_view_contract_check_end_to_end(data_source_test_helper: DataSourceTestHelper):
    """Run a full contract check (row_count + missing) against a view, not just metadata."""
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_views():
        pytest.skip("Views not supported")

    test_table = data_source_test_helper.ensure_test_table(simple_table)
    view_table = data_source_test_helper.create_view_from_test_table(test_table)

    data_source_test_helper.assert_contract_pass(
        test_table=view_table,
        contract_yaml_str="""
            columns:
              - name: name
                checks:
                  - missing:
            checks:
              - row_count:
                  threshold:
                    must_be: 2
        """,
    )


def test_materialized_view_contract_check_end_to_end(data_source_test_helper: DataSourceTestHelper):
    """Run a full contract check against a materialized view."""
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_materialized_views():
        pytest.skip("Materialized views not supported")

    test_table = data_source_test_helper.ensure_test_table(simple_table)
    mv_table = data_source_test_helper.create_materialized_view_from_test_table(test_table)

    data_source_test_helper.assert_contract_pass(
        test_table=mv_table,
        contract_yaml_str="""
            columns:
              - name: name
                checks:
                  - missing:
            checks:
              - row_count:
                  threshold:
                    must_be: 2
        """,
    )


# ---------------------------------------------------------------------------
# Type mapping round-trip: create → discover → map back to SodaDataTypeName
# ---------------------------------------------------------------------------

# Expected SodaDataTypeName for each column in all_types_table.
EXPECTED_TYPE_MAP = {
    "col_varchar": SodaDataTypeName.VARCHAR,
    "col_text": SodaDataTypeName.TEXT,
    "col_integer": SodaDataTypeName.INTEGER,
    "col_bigint": SodaDataTypeName.BIGINT,
    "col_smallint": SodaDataTypeName.SMALLINT,
    "col_float": SodaDataTypeName.FLOAT,
    "col_double": SodaDataTypeName.DOUBLE,
    "col_boolean": SodaDataTypeName.BOOLEAN,
    "col_date": SodaDataTypeName.DATE,
    "col_timestamp": SodaDataTypeName.TIMESTAMP,
    "col_timestamp_tz": SodaDataTypeName.TIMESTAMP_TZ,
    "col_numeric": SodaDataTypeName.NUMERIC,
    "col_decimal": SodaDataTypeName.DECIMAL,
    "col_char": SodaDataTypeName.CHAR,
    "col_time": SodaDataTypeName.TIME,
}


def test_all_types_round_trip(data_source_test_helper: DataSourceTestHelper):
    """Every SodaDataTypeName must survive a create→discover→map-back round-trip.

    Tighter than test_soda_data_types.py: this test asserts the exact expected
    SodaDataTypeName (with synonym awareness) for each column, not just that a
    mapping exists.
    """
    test_table = data_source_test_helper.ensure_test_table(all_types_table)
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    actual_columns: list[ColumnMetadata] = data_source_test_helper.data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table.dataset_prefix,
        dataset_name=test_table.unique_name,
    )

    assert len(actual_columns) == len(
        EXPECTED_TYPE_MAP
    ), f"Column count mismatch: expected {len(EXPECTED_TYPE_MAP)}, got {len(actual_columns)}"

    reverse_map = sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    for col in actual_columns:
        col_name = col.column_name.lower()
        expected_soda_type = EXPECTED_TYPE_MAP.get(col_name)
        assert expected_soda_type is not None, f"Unexpected column in metadata: {col_name}"

        ds_type_name = col.sql_data_type.name
        actual_soda_type = reverse_map.get(ds_type_name)
        assert (
            actual_soda_type is not None
        ), f"Column '{col_name}': data source type '{ds_type_name}' has no reverse mapping"
        assert sql_dialect.is_same_soda_data_type_with_synonyms(expected_soda_type, actual_soda_type), (
            f"Column '{col_name}': expected SodaDataType {expected_soda_type}, "
            f"got {actual_soda_type} (from DS type '{ds_type_name}')"
        )


# ---------------------------------------------------------------------------
# Type synonym bidirectionality
# ---------------------------------------------------------------------------


def test_type_synonyms_are_bidirectional(data_source_test_helper: DataSourceTestHelper):
    """For each data source type synonym, both the canonical and synonym names
    must map to the same SodaDataTypeName through the reverse mapping.

    This catches silent bugs where a type synonym is defined but the reverse
    mapping only recognizes the canonical form.
    """
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect
    synonym_lists = sql_dialect._get_data_type_name_synonyms()
    reverse_map = sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    mismatches = []
    for synonym_group in synonym_lists:
        # All names in a synonym group should resolve to the same SodaDataTypeName
        resolved = {}
        for type_name in synonym_group:
            soda_type = reverse_map.get(type_name.lower()) or reverse_map.get(type_name)
            if soda_type is not None:
                resolved[type_name] = soda_type

        if len(resolved) < 2:
            # Only one or zero names in this group have a reverse mapping — skip
            continue

        soda_types = set(resolved.values())
        # Allow synonym-aware comparison: all resolved types should be considered equivalent
        canonical = next(iter(soda_types))
        for type_name, soda_type in resolved.items():
            if not sql_dialect.is_same_soda_data_type_with_synonyms(canonical, soda_type):
                mismatches.append(
                    f"Synonym group {synonym_group}: '{type_name}' maps to {soda_type}, "
                    f"but others map to {canonical}"
                )

    assert mismatches == [], f"Type synonym bidirectionality broken:\n" + "\n".join(mismatches)


# ---------------------------------------------------------------------------
# Column type parameters: precision, scale, length
# ---------------------------------------------------------------------------


def test_column_type_parameters_preserved(data_source_test_helper: DataSourceTestHelper):
    """Column type parameters (length, precision, scale, datetime precision) must
    survive the create→discover round-trip for adapters that support them."""
    test_table = data_source_test_helper.ensure_test_table(typed_params_table)
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    actual_columns: list[ColumnMetadata] = data_source_test_helper.data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table.dataset_prefix,
        dataset_name=test_table.unique_name,
    )

    cols_by_name = {c.column_name.lower(): c for c in actual_columns}

    # character_maximum_length
    if sql_dialect.supports_data_type_character_maximum_length():
        varchar_col = cols_by_name.get("varchar_100")
        assert varchar_col is not None, "Column varchar_100 not found"
        if varchar_col.sql_data_type.character_maximum_length is not None:
            assert (
                varchar_col.sql_data_type.character_maximum_length == 100
            ), f"varchar_100: expected length 100, got {varchar_col.sql_data_type.character_maximum_length}"

        char_col = cols_by_name.get("char_10")
        assert char_col is not None, "Column char_10 not found"
        if char_col.sql_data_type.character_maximum_length is not None:
            assert (
                char_col.sql_data_type.character_maximum_length == 10
            ), f"char_10: expected length 10, got {char_col.sql_data_type.character_maximum_length}"

    # numeric_precision and numeric_scale
    if sql_dialect.supports_data_type_numeric_precision():
        numeric_col = cols_by_name.get("numeric_18_4")
        assert numeric_col is not None, "Column numeric_18_4 not found"
        if numeric_col.sql_data_type.numeric_precision is not None:
            assert (
                numeric_col.sql_data_type.numeric_precision == 18
            ), f"numeric_18_4: expected precision 18, got {numeric_col.sql_data_type.numeric_precision}"

        decimal_col = cols_by_name.get("decimal_10_2")
        assert decimal_col is not None, "Column decimal_10_2 not found"
        if decimal_col.sql_data_type.numeric_precision is not None:
            assert (
                decimal_col.sql_data_type.numeric_precision == 10
            ), f"decimal_10_2: expected precision 10, got {decimal_col.sql_data_type.numeric_precision}"

    if sql_dialect.supports_data_type_numeric_scale():
        numeric_col = cols_by_name.get("numeric_18_4")
        assert numeric_col is not None, "Column numeric_18_4 not found"
        if numeric_col.sql_data_type.numeric_scale is not None:
            assert (
                numeric_col.sql_data_type.numeric_scale == 4
            ), f"numeric_18_4: expected scale 4, got {numeric_col.sql_data_type.numeric_scale}"

        decimal_col = cols_by_name.get("decimal_10_2")
        assert decimal_col is not None, "Column decimal_10_2 not found"
        if decimal_col.sql_data_type.numeric_scale is not None:
            assert (
                decimal_col.sql_data_type.numeric_scale == 2
            ), f"decimal_10_2: expected scale 2, got {decimal_col.sql_data_type.numeric_scale}"

    # datetime_precision
    if sql_dialect.supports_data_type_datetime_precision():
        ts_col = cols_by_name.get("ts_precision_3")
        assert ts_col is not None, "Column ts_precision_3 not found"
        if ts_col.sql_data_type.datetime_precision is not None:
            assert (
                ts_col.sql_data_type.datetime_precision == 3
            ), f"ts_precision_3: expected datetime_precision 3, got {ts_col.sql_data_type.datetime_precision}"

        ts_tz_col = cols_by_name.get("ts_tz_precision_6")
        assert ts_tz_col is not None, "Column ts_tz_precision_6 not found"
        if ts_tz_col.sql_data_type.datetime_precision is not None:
            assert (
                ts_tz_col.sql_data_type.datetime_precision == 6
            ), f"ts_tz_precision_6: expected datetime_precision 6, got {ts_tz_col.sql_data_type.datetime_precision}"


# ---------------------------------------------------------------------------
# Every SodaDataTypeName has both forward and reverse mappings
# ---------------------------------------------------------------------------


def test_every_soda_type_has_forward_mapping(data_source_test_helper: DataSourceTestHelper):
    """Every SodaDataTypeName must have a forward mapping (Soda→data source)."""
    forward_map = (
        data_source_test_helper.data_source_impl.sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()
    )
    unmapped = [str(t) for t in SodaDataTypeName if t not in forward_map]
    assert unmapped == [], f"SodaDataTypeNames with no forward mapping: {unmapped}"


def test_every_forward_mapped_type_has_reverse(data_source_test_helper: DataSourceTestHelper):
    """Every data source type produced by the forward mapping must have a reverse mapping."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    forward_map = sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()
    reverse_map = sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    unmapped = []
    for soda_type, ds_type in forward_map.items():
        ds_type_lower = ds_type.lower() if isinstance(ds_type, str) else ds_type
        if ds_type not in reverse_map and ds_type_lower not in reverse_map:
            # Check synonyms
            canonical = sql_dialect._data_type_name_synonym_mappings.get(ds_type_lower, ds_type_lower)
            if canonical not in reverse_map:
                unmapped.append(f"{soda_type} → '{ds_type}' (no reverse)")

    assert unmapped == [], f"Forward-mapped types with no reverse mapping:\n" + "\n".join(unmapped)
