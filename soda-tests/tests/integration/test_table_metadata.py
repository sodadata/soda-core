import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.metadata_types import (
    ColumnMetadata,
    SodaDataTypeName,
    SqlDataType,
)
from soda_core.common.sql_ast import CREATE_TABLE, CREATE_TABLE_COLUMN
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import (
    FullyQualifiedViewName,
    TableType,
)
from soda_core.common.statements.table_types import (
    FullyQualifiedMaterializedViewName,
    FullyQualifiedTableName,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("metadata")
    .column_varchar(name="varchar_default")
    .column_varchar(name="varchar_w_length", character_maximum_length=255)
    .column_integer(name="integer_default")
    .column_numeric(name="numeric_default")
    .column_numeric(name="numeric_w_precision", numeric_precision=10)
    .column_numeric(name="numeric_w_precision_and_scale", numeric_precision=10, numeric_scale=2)
    .column_timestamp(name="ts_default")
    .column_timestamp(name="ts_w_precision", datetime_precision=2)
    .column_timestamp_tz(name="ts_tz_default")
    .column_timestamp_tz(name="ts_tz_w_precision", datetime_precision=4)
    .build()
)


def __verify_table_metadata(actual_columns: list[ColumnMetadata], sql_dialect: SqlDialect):
    sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.VARCHAR)

    actual_txt_default: ColumnMetadata = actual_columns[0]
    assert actual_txt_default.column_name == "varchar_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.VARCHAR)
        ),
        actual=actual_txt_default.sql_data_type,
    )

    actual_txt_w_length: ColumnMetadata = actual_columns[1]
    assert actual_txt_w_length.column_name == "varchar_w_length"
    length = 255 if sql_dialect.supports_data_type_character_maximum_length() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.VARCHAR),
            character_maximum_length=length,
        ),
        actual=actual_txt_w_length.sql_data_type,
    )

    actual_integer_default: ColumnMetadata = actual_columns[2]
    assert actual_integer_default.column_name == "integer_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.INTEGER),
        ),
        actual=actual_integer_default.sql_data_type,
    )

    actual_numeric_default: ColumnMetadata = actual_columns[3]
    assert actual_numeric_default.column_name == "numeric_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.NUMERIC),
            numeric_precision=sql_dialect.default_numeric_precision(),
            numeric_scale=sql_dialect.default_numeric_scale(),
        ),
        actual=actual_numeric_default.sql_data_type,
    )

    actual_numeric_w_precision: ColumnMetadata = actual_columns[4]
    assert actual_numeric_w_precision.column_name == "numeric_w_precision"
    precision = 10 if sql_dialect.supports_data_type_numeric_precision() else None
    scale = 0 if sql_dialect.supports_data_type_numeric_scale() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.NUMERIC),
            numeric_precision=precision,
            numeric_scale=scale,
        ),
        actual=actual_numeric_w_precision.sql_data_type,
    )

    actual_numeric_w_precision_and_scale: ColumnMetadata = actual_columns[5]
    assert actual_numeric_w_precision_and_scale.column_name == "numeric_w_precision_and_scale"
    precision = 10 if sql_dialect.supports_data_type_numeric_precision() else None
    scale = 2 if sql_dialect.supports_data_type_numeric_scale() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.NUMERIC),
            numeric_precision=precision,
            numeric_scale=scale,
        ),
        actual=actual_numeric_w_precision_and_scale.sql_data_type,
    )

    actual_ts_default: ColumnMetadata = actual_columns[6]
    assert actual_ts_default.column_name == "ts_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.TIMESTAMP),
        ),
        actual=actual_ts_default.sql_data_type,
    )

    actual_ts_w_precision: ColumnMetadata = actual_columns[7]
    assert actual_ts_w_precision.column_name == "ts_w_precision"
    precision = 2 if sql_dialect.supports_data_type_datetime_precision() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.TIMESTAMP),
            datetime_precision=precision,
        ),
        actual=actual_ts_w_precision.sql_data_type,
    )


# Composite primary key (two columns) so this also exercises multi-column PK DDL and
# multi-row information_schema introspection, not just the single-column case.
primary_keys_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("primary_keys")
    .column_integer(name="tenant_id")
    .column_integer(name="id")
    .column_varchar(name="label")
    .primary_key(["tenant_id", "id"])
    .build()
)

# Single-column primary key table, used together with the composite-PK table to
# exercise multi-table grouping in a single bulk query.
single_primary_key_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("primary_key_single")
    .column_integer(name="id")
    .column_varchar(name="label")
    .primary_key(["id"])
    .build()
)


def _find_pk_key(primary_keys_by_table: dict[str, set[str]], table_name: str) -> str | None:
    """Find the key matching the table name case-insensitively (data sources key the result by their
    own information_schema casing). Mirrors _find_table_key in test_all_columns_metadata_for_schema.py."""
    for key in primary_keys_by_table:
        if key.lower() == table_name.lower():
            return key
    return None


def test_primary_keys_metadata(data_source_test_helper: DataSourceTestHelper):
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_primary_keys():
        pytest.skip("data source does not support primary key introspection")

    composite_table = data_source_test_helper.ensure_test_table(primary_keys_test_table_specification)
    single_table = data_source_test_helper.ensure_test_table(single_primary_key_test_table_specification)

    actual: dict[str, set[str]] = data_source_test_helper.data_source_impl.get_primary_keys(
        dataset_prefixes=composite_table.dataset_prefix,
        dataset_names=[composite_table.unique_name, single_table.unique_name],
    )

    # Look up case-insensitively: the result is keyed by the data source's own information_schema
    # casing, which can differ from the requested name (Databricks lowercases identifiers even for a
    # quoted CREATE). Mirrors _find_table_key in test_all_columns_metadata_for_schema.py.
    composite_key = _find_pk_key(actual, composite_table.unique_name)
    assert composite_key is not None, f"Table {composite_table.unique_name} not found. Available: {list(actual.keys())}"
    assert actual[composite_key] == {"tenant_id", "id"}

    single_key = _find_pk_key(actual, single_table.unique_name)
    assert single_key is not None, f"Table {single_table.unique_name} not found. Available: {list(actual.keys())}"
    assert actual[single_key] == {"id"}


# Single-column primary key on a DIFFERENT column than the default-schema table, used by the
# cross-schema leak test below. Same table_purpose collides on the builder's purpose-uniqueness
# check, so this spec is only ever materialised by hand into a second schema (never via the builder
# registry) — see test_primary_keys_metadata_do_not_leak_across_schemas.
cross_schema_other_pk_column = "tenant_id"


def test_primary_keys_metadata_do_not_leak_across_schemas(data_source_test_helper: DataSourceTestHelper):
    """Regression for the bulk-PK JOIN collapsing constraint namespaces.

    constraint_name is unique only within (constraint_catalog, constraint_schema). Postgres
    auto-names every PK "<table>_pkey", so two tables that share a name in two different schemas
    share a constraint name. If the table_constraints -> key_column_usage JOIN matches on
    constraint_name ONLY (the original v4 bug), querying one schema pulls in the OTHER schema's PK
    columns. This test creates that exact collision and asserts each schema reports only its own PK.
    """
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_primary_keys():
        pytest.skip("data source does not support primary key introspection")

    data_source_impl = data_source_test_helper.data_source_impl
    sql_dialect = data_source_impl.sql_dialect

    # Table in the default (helper-managed) schema: PK on `id`.
    primary_table = data_source_test_helper.ensure_test_table(single_primary_key_test_table_specification)
    default_prefix = primary_table.dataset_prefix
    table_name = primary_table.unique_name

    # Second schema, sharing the database/catalog but a distinct schema name, holding a table with
    # the SAME name (so the auto-generated PK constraint name collides) but a DIFFERENT PK column.
    database_name = default_prefix[0]
    other_schema_name = f"{default_prefix[1]}_pkleak"
    other_prefix = [database_name, other_schema_name]

    def _create_table_sql(prefix: list[str]) -> str:
        qualified_name = sql_dialect.qualify_dataset_name(prefix, table_name)
        return sql_dialect.build_create_table_sql(
            CREATE_TABLE(
                fully_qualified_table_name=qualified_name,
                columns=[
                    CREATE_TABLE_COLUMN(
                        name=cross_schema_other_pk_column,
                        type=sql_dialect.map_test_sql_data_type_to_data_source(
                            SqlDataType(name=SodaDataTypeName.INTEGER)
                        ),
                    ),
                    CREATE_TABLE_COLUMN(
                        name="id",
                        type=sql_dialect.map_test_sql_data_type_to_data_source(
                            SqlDataType(name=SodaDataTypeName.INTEGER)
                        ),
                    ),
                ],
                primary_key_column_names=[cross_schema_other_pk_column],
            )
        )

    data_source_test_helper.drop_schema_if_exists(other_schema_name)
    try:
        data_source_impl.execute_update(sql_dialect.create_schema_if_not_exists_sql(other_prefix))
        data_source_impl.execute_update(_create_table_sql(other_prefix))
        data_source_impl.data_source_connection.commit()

        # Query the DEFAULT schema. With the buggy constraint-name-only JOIN, the other schema's
        # PK column (`tenant_id`) would leak in; with the fix, only `id` is returned.
        default_pks: dict[str, set[str]] = data_source_impl.get_primary_keys(
            dataset_prefixes=default_prefix,
            dataset_names=[table_name],
        )
        default_key = _find_pk_key(default_pks, table_name)
        assert (
            default_key is not None
        ), f"Table {table_name} not found in default schema. Available: {list(default_pks.keys())}"
        assert default_pks[default_key] == {
            "id"
        }, f"Default-schema PK leaked columns from the other schema: {default_pks[default_key]}"

        # Symmetrically, the other schema must report only its own PK (`tenant_id`), not `id`.
        other_pks: dict[str, set[str]] = data_source_impl.get_primary_keys(
            dataset_prefixes=other_prefix,
            dataset_names=[table_name],
        )
        other_key = _find_pk_key(other_pks, table_name)
        assert (
            other_key is not None
        ), f"Table {table_name} not found in other schema. Available: {list(other_pks.keys())}"
        assert other_pks[other_key] == {
            cross_schema_other_pk_column
        }, f"Other-schema PK leaked columns from the default schema: {other_pks[other_key]}"
    finally:
        # drop_schema_if_exists issues DROP SCHEMA ... CASCADE, which also removes the table.
        data_source_test_helper.drop_schema_if_exists(other_schema_name)
        data_source_impl.data_source_connection.commit()


# Note: this test is for metadata related items only. For the full datatypes, please see test_soda_data_types.py
def test_table_metadata(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    actual_columns: list[ColumnMetadata] = data_source_test_helper.data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table.dataset_prefix, dataset_name=test_table.unique_name
    )

    __verify_table_metadata(actual_columns, sql_dialect)


def test_view_metadata(data_source_test_helper: DataSourceTestHelper):
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_views():
        pytest.skip("This data source does not support views")
    # This is the same as the test_table_metadata test, but we create a view from the test table and then get the metadata from the view.
    # So we verify if the metadata query is able to get the data from the view.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    view_name = data_source_test_helper.create_view_from_test_table(test_table).unique_name

    actual_columns: list[ColumnMetadata] = data_source_test_helper.data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table.dataset_prefix, dataset_name=view_name
    )

    __verify_table_metadata(actual_columns, sql_dialect)


def test_view_not_detected_by_table_metadata(
    data_source_test_helper: DataSourceTestHelper,
):
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_views():
        pytest.skip("This data source does not support views")
    # This test verifies the "default behavior" of the metadata tables query, which is to return only tables.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    _ = data_source_test_helper.create_view_from_test_table(test_table)

    table_metadata_query = data_source_test_helper.data_source_impl.create_metadata_tables_query()
    table_metadata = table_metadata_query.execute(
        database_name=data_source_test_helper.extract_database_from_prefix(),
        schema_name=data_source_test_helper.extract_schema_from_prefix(),
        include_table_name_like_filters=["SODATEST_%"],
    )

    # No element of the results can be a FullyQualifiedViewName
    for element in table_metadata:
        assert not isinstance(element, FullyQualifiedViewName)


def test_view_detected_by_table_metadata(data_source_test_helper: DataSourceTestHelper):
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_views():
        pytest.skip("This data source does not support views")
    # This test verifies that the metadata tables query is able to return only views.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    view_name = data_source_test_helper.create_view_from_test_table(test_table).unique_name

    table_metadata_query = data_source_test_helper.data_source_impl.create_metadata_tables_query()
    table_metadata = table_metadata_query.execute(
        database_name=data_source_test_helper.extract_database_from_prefix(),
        schema_name=data_source_test_helper.extract_schema_from_prefix(),
        types_to_return=[TableType.VIEW],
    )

    # All elements of the results must be a FullyQualifiedViewName
    # Also check that the name of the view is found
    view_name_found = False
    for element in table_metadata:
        assert isinstance(element, FullyQualifiedViewName)
        if element.view_name == view_name:
            view_name_found = True
    assert view_name_found


def test_materialized_view_detected_by_table_metadata(
    data_source_test_helper: DataSourceTestHelper,
):
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_materialized_views():
        pytest.skip("This data source does not support materialized views")
    # This test verifies that the metadata tables query is able to return only materialized views.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    view_name = data_source_test_helper.create_materialized_view_from_test_table(test_table).unique_name

    table_metadata_query = data_source_test_helper.data_source_impl.create_metadata_tables_query()
    table_metadata = table_metadata_query.execute(
        database_name=data_source_test_helper.extract_database_from_prefix(),
        schema_name=data_source_test_helper.extract_schema_from_prefix(),
        types_to_return=[TableType.MATERIALIZED_VIEW],
    )

    # All elements of the results must be a FullyQualifiedMaterializedViewName
    # Also check that the name of the view is found
    view_name_found = False
    for element in table_metadata:
        assert isinstance(element, FullyQualifiedMaterializedViewName)
        if element.materialized_view_name == view_name:
            view_name_found = True
    assert view_name_found, f"Materialized view {view_name} not found in metadata query results"


def test_materialized_view_not_detected_by_table_metadata(
    data_source_test_helper: DataSourceTestHelper,
):
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_materialized_views():
        pytest.skip("This data source does not support materialized views")
    # This test verifies that materialized views do not appear when querying for tables only.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    _ = data_source_test_helper.create_materialized_view_from_test_table(test_table)

    table_metadata_query = data_source_test_helper.data_source_impl.create_metadata_tables_query()
    table_metadata = table_metadata_query.execute(
        database_name=data_source_test_helper.extract_database_from_prefix(),
        schema_name=data_source_test_helper.extract_schema_from_prefix(),
        include_table_name_like_filters=["SODATEST_%"],
    )

    # No element of the results can be a FullyQualifiedMaterializedViewName
    for element in table_metadata:
        assert not isinstance(element, FullyQualifiedMaterializedViewName)


def test_mixed_types_detected_by_table_metadata(
    data_source_test_helper: DataSourceTestHelper,
):
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_materialized_views():
        pytest.skip("This data source does not support materialized views")
    # This test verifies that querying for both tables and materialized views returns both types.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    mv_name = data_source_test_helper.create_materialized_view_from_test_table(test_table).unique_name

    table_metadata_query = data_source_test_helper.data_source_impl.create_metadata_tables_query()
    table_metadata = table_metadata_query.execute(
        database_name=data_source_test_helper.extract_database_from_prefix(),
        schema_name=data_source_test_helper.extract_schema_from_prefix(),
        include_table_name_like_filters=["SODATEST_%"],
        types_to_return=[TableType.TABLE, TableType.MATERIALIZED_VIEW],
    )

    found_table = False
    found_mv = False
    for element in table_metadata:
        assert isinstance(element, (FullyQualifiedTableName, FullyQualifiedMaterializedViewName))
        if isinstance(element, FullyQualifiedTableName):
            found_table = True
        if isinstance(element, FullyQualifiedMaterializedViewName) and element.materialized_view_name == mv_name:
            found_mv = True
    assert found_table, "No tables found in mixed-type metadata query results"
    assert found_mv, f"Materialized view {mv_name} not found in mixed-type metadata query results"
