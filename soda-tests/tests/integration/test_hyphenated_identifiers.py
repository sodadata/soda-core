from helpers.data_source_test_helper import DataSourceTestHelper
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
