from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    DROP_TABLE_IF_EXISTS,
)
from soda_core.common.sql_dialect import FROM, SELECT, STAR


def test_hyphenated_identifier_is_quoted(data_source_test_helper: DataSourceTestHelper):
    """Every dialect must quote identifiers so that hyphens are not parsed as minus operators."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    quoted = sql_dialect.quote_default("my-table")
    assert quoted is not None, "quote_default returned None for a valid identifier"
    assert quoted != "my-table", f"{sql_dialect.__class__.__name__}.quote_default returned a bare identifier"
    assert "my-table" in quoted, f"Hyphenated name not preserved in quoted result: {quoted}"


def test_hyphenated_table_name_in_select(data_source_test_helper: DataSourceTestHelper):
    """SELECT from a hyphenated table name must produce quoted identifier in the SQL."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    sql = sql_dialect.build_select_sql([SELECT(STAR()), FROM("my-table")])
    assert "my-table" in sql, f"Hyphenated name missing from generated SQL: {sql}"
    # The bare token "my-table" without surrounding quotes would be parsed as "my" minus "table"
    assert (
        " my-table;" not in sql and " my-table\n" not in sql
    ), f"{sql_dialect.__class__.__name__} produced unquoted hyphenated identifier in SELECT: {sql}"


def test_ddl_quotes_hyphenated_table_name(data_source_test_helper: DataSourceTestHelper):
    """DDL statements must quote hyphenated identifiers using the dialect's DDL quoting."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    fqn = sql_dialect.quote_default("my-table")

    create_sql = sql_dialect.build_create_table_sql(
        CREATE_TABLE_IF_NOT_EXISTS(
            fully_qualified_table_name=fqn,
            columns=[CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name=SodaDataTypeName.INTEGER))],
        )
    )
    drop_sql = sql_dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=fqn))

    for label, sql in [("CREATE TABLE", create_sql), ("DROP TABLE", drop_sql)]:
        assert "my-table" in sql, f"{label}: hyphenated name missing from SQL: {sql}"
        assert " my-table " not in sql, f"{label}: unquoted hyphenated identifier: {sql}"


def test_ddl_and_dml_quoting_are_consistent(data_source_test_helper: DataSourceTestHelper):
    """quote_for_ddl must return a quoted identifier (not None or bare) for valid input."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    ddl_quoted = sql_dialect.quote_for_ddl("my-table")
    assert ddl_quoted is not None, "quote_for_ddl returned None for a valid identifier"
    assert ddl_quoted != "my-table", "quote_for_ddl returned a bare identifier"
    assert "my-table" in ddl_quoted, f"Hyphenated name not preserved in DDL quoted result: {ddl_quoted}"
