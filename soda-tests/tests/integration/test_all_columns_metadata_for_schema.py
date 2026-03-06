import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import (
    ColumnMetadata,
    SodaDataTypeName,
    SqlDataType,
)
from soda_core.common.sql_dialect import SqlDialect

table_a_specification = (
    TestTableSpecification.builder()
    .table_purpose("all_cols_a")
    .column_varchar(name="name")
    .column_integer(name="age")
    .column_numeric(name="score", numeric_precision=10, numeric_scale=2)
    .build()
)

table_b_specification = (
    TestTableSpecification.builder()
    .table_purpose("all_cols_b")
    .column_varchar(name="city")
    .column_date(name="created")
    .build()
)


def test_get_all_columns_metadata_for_schema(data_source_test_helper: DataSourceTestHelper):
    test_table_a = data_source_test_helper.ensure_test_table(table_a_specification)
    test_table_b = data_source_test_helper.ensure_test_table(table_b_specification)
    data_source_impl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect

    # Use force_fetch_all=True so this test works for all data sources (including those without bulk support)
    # Pass table_names to only fetch columns for the test tables (faster for per-table fallback)
    all_columns: dict[str, list[ColumnMetadata]] = data_source_impl.get_all_columns_metadata_for_schema(
        prefixes=test_table_a.dataset_prefix,
        force_fetch_all=True,
        table_names=[test_table_a.unique_name, test_table_b.unique_name],
    )

    _assert_schema_columns(all_columns, test_table_a, test_table_b, data_source_impl, sql_dialect)


def test_bulk_columns_metadata_returns_empty_when_not_available(data_source_test_helper: DataSourceTestHelper):
    """When bulk_columns_metadata_available is False, get_all_columns_metadata_for_schema returns empty dict."""
    test_table_a = data_source_test_helper.ensure_test_table(table_a_specification)
    data_source_impl = data_source_test_helper.data_source_impl

    if data_source_impl.bulk_columns_metadata_available:
        pytest.skip("Data source supports bulk metadata — this test targets non-bulk data sources")

    all_columns = data_source_impl.get_all_columns_metadata_for_schema(
        prefixes=test_table_a.dataset_prefix,
    )
    assert all_columns == {}, "Expected empty dict when bulk_columns_metadata_available is False"


def test_force_fetches_columns_when_bulk_not_available(data_source_test_helper: DataSourceTestHelper):
    """When bulk_columns_metadata_available is False, force_fetch_all=True falls back to per-table iteration."""
    test_table_a = data_source_test_helper.ensure_test_table(table_a_specification)
    test_table_b = data_source_test_helper.ensure_test_table(table_b_specification)
    data_source_impl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect

    if data_source_impl.bulk_columns_metadata_available:
        pytest.skip("Data source supports bulk metadata — this test targets non-bulk data sources")

    all_columns = data_source_impl.get_all_columns_metadata_for_schema(
        prefixes=test_table_a.dataset_prefix,
        force_fetch_all=True,
        table_names=[test_table_a.unique_name, test_table_b.unique_name],
    )

    _assert_schema_columns(all_columns, test_table_a, test_table_b, data_source_impl, sql_dialect)


def test_table_names_filter(data_source_test_helper: DataSourceTestHelper):
    """When table_names is provided, only those tables should appear in the result."""
    test_table_a = data_source_test_helper.ensure_test_table(table_a_specification)
    test_table_b = data_source_test_helper.ensure_test_table(table_b_specification)
    data_source_impl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect

    # Request only table A
    result = data_source_impl.get_all_columns_metadata_for_schema(
        prefixes=test_table_a.dataset_prefix,
        force_fetch_all=True,
        table_names=[test_table_a.unique_name],
    )

    # Table A should be present with correct columns
    table_a_key = _find_table_key(result, test_table_a.unique_name)
    assert table_a_key is not None, f"Table {test_table_a.unique_name} not found. Available: {list(result.keys())}"
    cols_a = result[table_a_key]
    assert len(cols_a) == 3
    _assert_column(cols_a[0], "name", SodaDataTypeName.VARCHAR, sql_dialect)
    _assert_column(cols_a[1], "age", SodaDataTypeName.INTEGER, sql_dialect)
    _assert_column(cols_a[2], "score", SodaDataTypeName.NUMERIC, sql_dialect, numeric_precision=10, numeric_scale=2)

    # Table B should NOT be present
    table_b_key = _find_table_key(result, test_table_b.unique_name)
    assert table_b_key is None, (
        f"Table {test_table_b.unique_name} should not be in result when not in table_names filter. "
        f"Available: {list(result.keys())}"
    )


def test_get_columns_metadata_for_single_table(data_source_test_helper: DataSourceTestHelper):
    test_table_a = data_source_test_helper.ensure_test_table(table_a_specification)
    test_table_b = data_source_test_helper.ensure_test_table(table_b_specification)
    data_source_impl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect

    # Verify table A columns via single-table query (uses build_columns_metadata_query_str)
    cols_a = data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table_a.dataset_prefix,
        dataset_name=test_table_a.unique_name,
    )
    assert len(cols_a) == 3
    _assert_column(cols_a[0], "name", SodaDataTypeName.VARCHAR, sql_dialect)
    _assert_column(cols_a[1], "age", SodaDataTypeName.INTEGER, sql_dialect)
    _assert_column(cols_a[2], "score", SodaDataTypeName.NUMERIC, sql_dialect, numeric_precision=10, numeric_scale=2)

    # Verify table B columns via single-table query
    cols_b = data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table_b.dataset_prefix,
        dataset_name=test_table_b.unique_name,
    )
    assert len(cols_b) == 2
    _assert_column(cols_b[0], "city", SodaDataTypeName.VARCHAR, sql_dialect)
    _assert_column(cols_b[1], "created", SodaDataTypeName.DATE, sql_dialect)

    # Verify table A query does not return table B columns and vice versa
    col_names_a = {c.column_name.lower() for c in cols_a}
    col_names_b = {c.column_name.lower() for c in cols_b}
    assert col_names_a.isdisjoint(
        col_names_b
    ), f"Single-table queries should return distinct columns, but found overlap: {col_names_a & col_names_b}"


def _assert_schema_columns(
    all_columns: dict[str, list[ColumnMetadata]],
    test_table_a,
    test_table_b,
    data_source_impl: DataSourceImpl,
    sql_dialect: SqlDialect,
):
    """Shared assertions for schema-wide column metadata."""
    table_a_key = _find_table_key(all_columns, test_table_a.unique_name)
    assert table_a_key is not None, (
        f"Table {test_table_a.unique_name} not found in schema metadata. "
        f"Available tables: {list(all_columns.keys())}"
    )
    cols_a = all_columns[table_a_key]
    assert len(cols_a) == 3
    _assert_column(cols_a[0], "name", SodaDataTypeName.VARCHAR, sql_dialect)
    _assert_column(cols_a[1], "age", SodaDataTypeName.INTEGER, sql_dialect)
    _assert_column(cols_a[2], "score", SodaDataTypeName.NUMERIC, sql_dialect, numeric_precision=10, numeric_scale=2)

    table_b_key = _find_table_key(all_columns, test_table_b.unique_name)
    assert table_b_key is not None, (
        f"Table {test_table_b.unique_name} not found in schema metadata. "
        f"Available tables: {list(all_columns.keys())}"
    )
    cols_b = all_columns[table_b_key]
    assert len(cols_b) == 2
    _assert_column(cols_b[0], "city", SodaDataTypeName.VARCHAR, sql_dialect)
    _assert_column(cols_b[1], "created", SodaDataTypeName.DATE, sql_dialect)

    # Verify consistency: single-table query returns the same data as schema-wide query
    for test_table, cols_from_schema in [(test_table_a, cols_a), (test_table_b, cols_b)]:
        single_cols = data_source_impl.get_columns_metadata(
            dataset_prefixes=test_table.dataset_prefix,
            dataset_name=test_table.unique_name,
        )
        assert len(single_cols) == len(cols_from_schema), (
            f"Column count mismatch for {test_table.unique_name}: "
            f"single-table={len(single_cols)}, schema-wide={len(cols_from_schema)}"
        )
        for i, col in enumerate(single_cols):
            assert col.column_name.lower() == cols_from_schema[i].column_name.lower()
            assert sql_dialect.is_same_data_type_for_schema_check(
                expected=col.sql_data_type,
                actual=cols_from_schema[i].sql_data_type,
            )


def _assert_column(
    actual: ColumnMetadata,
    expected_name: str,
    expected_soda_type: str,
    sql_dialect: SqlDialect,
    numeric_precision: int | None = None,
    numeric_scale: int | None = None,
):
    assert actual.column_name.lower() == expected_name
    expected = SqlDataType(
        name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(expected_soda_type),
        numeric_precision=numeric_precision if sql_dialect.supports_data_type_numeric_precision() else None,
        numeric_scale=numeric_scale if sql_dialect.supports_data_type_numeric_scale() else None,
    )
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=expected, actual=actual.sql_data_type
    ), f"Type mismatch for column '{expected_name}': expected={expected}, actual={actual.sql_data_type}"


def _find_table_key(columns_by_table: dict[str, list[ColumnMetadata]], table_name: str) -> str | None:
    """Find the key in the dict matching the table name (case-insensitive)."""
    for key in columns_by_table:
        if key.lower() == table_name.lower():
            return key
    return None
