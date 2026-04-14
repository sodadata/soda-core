"""
Adapter Conformance Tests: Identifier Quoting

Validates that every adapter correctly quotes identifiers containing special
characters in both DDL (CREATE TABLE) and DML (SELECT, INSERT) paths.
This is the #1 source of field bugs (~30% of historical fixes).

These tests go beyond the existing dialect-level tests in test_hyphenated_identifiers.py
by running full end-to-end contract checks — creating tables with problematic column names,
inserting data, and executing checks against them.

See: projects/enhancements/common_bugs_tests/historical-bug-analysis.md
"""

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

# ---------------------------------------------------------------------------
# Test tables
# ---------------------------------------------------------------------------

reserved_words_table = (
    TestTableSpecification.builder()
    .table_purpose("conf_reserved_words")
    .column_varchar("select")
    .column_varchar("table")
    .column_varchar("order")
    .column_varchar("group")
    .column_integer("count")
    .rows(
        [
            ("a", "t1", "asc", "g1", 1),
            ("b", "t2", "desc", "g2", 2),
            ("c", "t3", "asc", "g1", 3),
        ]
    )
    .build()
)

hyphenated_columns_table = (
    TestTableSpecification.builder()
    .table_purpose("conf_hyphenated_cols")
    .column_varchar("first-name")
    .column_varchar("last-name")
    .column_integer("row-id")
    .rows(
        [
            ("Alice", "Smith", 1),
            ("Bob", "Jones", 2),
            (None, "Brown", 3),
        ]
    )
    .build()
)

mixed_case_table = (
    TestTableSpecification.builder()
    .table_purpose("conf_mixed_case")
    .column_varchar("FirstName")
    .column_varchar("LastName")
    .column_integer("AccountBalance")
    .rows(
        [
            ("Alice", "Smith", 100),
            ("Bob", "Jones", 200),
            ("Charlie", "Brown", 300),
        ]
    )
    .build()
)


# ---------------------------------------------------------------------------
# Reserved SQL words as column names
# ---------------------------------------------------------------------------


def test_reserved_word_columns_row_count(data_source_test_helper: DataSourceTestHelper):
    """Table creation and row_count check must work with reserved-word column names."""
    test_table = data_source_test_helper.ensure_test_table(reserved_words_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
    )


def test_reserved_word_columns_missing_check(data_source_test_helper: DataSourceTestHelper):
    """Missing check must work on columns named with SQL reserved words."""
    test_table = data_source_test_helper.ensure_test_table(reserved_words_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: select
                checks:
                  - missing:
              - name: table
                checks:
                  - missing:
              - name: order
                checks:
                  - missing:
              - name: group
                checks:
                  - missing:
            checks:
              - row_count:
        """,
    )


def test_reserved_word_columns_aggregate_check(data_source_test_helper: DataSourceTestHelper):
    """Aggregate check (SUM) must work on a column named 'count' (reserved word)."""
    test_table = data_source_test_helper.ensure_test_table(reserved_words_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: count
                checks:
                  - aggregate:
                      function: sum
                      threshold:
                        must_be: 6
            checks:
              - row_count:
        """,
    )


def test_reserved_word_columns_schema_check(data_source_test_helper: DataSourceTestHelper):
    """Schema check must discover columns even when they are named with reserved words."""
    test_table = data_source_test_helper.ensure_test_table(reserved_words_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
                  allow_extra_columns: true
            columns:
              - name: select
                data_type: {test_table.data_type('select')}
              - name: count
                data_type: {test_table.data_type('count')}
        """,
    )


# ---------------------------------------------------------------------------
# Hyphenated column names (end-to-end)
# ---------------------------------------------------------------------------


def test_hyphenated_columns_row_count(data_source_test_helper: DataSourceTestHelper):
    """Table creation and row_count check with hyphenated column names."""
    test_table = data_source_test_helper.ensure_test_table(hyphenated_columns_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
    )


def test_hyphenated_columns_missing_detects_null(data_source_test_helper: DataSourceTestHelper):
    """Missing check must correctly detect the NULL in 'first-name' column."""
    test_table = data_source_test_helper.ensure_test_table(hyphenated_columns_table)
    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: first-name
                checks:
                  - missing:
            checks:
              - row_count:
        """,
    )


def test_hyphenated_columns_aggregate(data_source_test_helper: DataSourceTestHelper):
    """Aggregate check on a hyphenated integer column."""
    test_table = data_source_test_helper.ensure_test_table(hyphenated_columns_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: row-id
                checks:
                  - aggregate:
                      function: sum
                      threshold:
                        must_be: 6
            checks:
              - row_count:
        """,
    )


def test_hyphenated_columns_schema_check(data_source_test_helper: DataSourceTestHelper):
    """Schema check must discover hyphenated column names correctly."""
    test_table = data_source_test_helper.ensure_test_table(hyphenated_columns_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: first-name
                data_type: {test_table.data_type('first-name')}
              - name: last-name
                data_type: {test_table.data_type('last-name')}
              - name: row-id
                data_type: {test_table.data_type('row-id')}
        """,
    )


# ---------------------------------------------------------------------------
# Mixed-case (CamelCase) column names
# ---------------------------------------------------------------------------


def test_mixed_case_columns_row_count(data_source_test_helper: DataSourceTestHelper):
    """Row count check with CamelCase column names."""
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_case_sensitive_column_names():
        pytest.skip("Case sensitive column names not supported")
    test_table = data_source_test_helper.ensure_test_table(mixed_case_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
    )


def test_mixed_case_columns_missing_check(data_source_test_helper: DataSourceTestHelper):
    """Missing check referencing CamelCase column names."""
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_case_sensitive_column_names():
        pytest.skip("Case sensitive column names not supported")
    test_table = data_source_test_helper.ensure_test_table(mixed_case_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: FirstName
                checks:
                  - missing:
              - name: LastName
                checks:
                  - missing:
            checks:
              - row_count:
        """,
    )


def test_mixed_case_columns_aggregate(data_source_test_helper: DataSourceTestHelper):
    """Aggregate check on a CamelCase integer column."""
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_case_sensitive_column_names():
        pytest.skip("Case sensitive column names not supported")
    test_table = data_source_test_helper.ensure_test_table(mixed_case_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: AccountBalance
                checks:
                  - aggregate:
                      function: avg
                      threshold:
                        must_be: 200
            checks:
              - row_count:
        """,
    )


def test_mixed_case_columns_schema_preserves_case(data_source_test_helper: DataSourceTestHelper):
    """Schema check must preserve CamelCase column names."""
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_case_sensitive_column_names():
        pytest.skip("Case sensitive column names not supported")
    test_table = data_source_test_helper.ensure_test_table(mixed_case_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: FirstName
                data_type: {test_table.data_type('FirstName')}
              - name: LastName
                data_type: {test_table.data_type('LastName')}
              - name: AccountBalance
                data_type: {test_table.data_type('AccountBalance')}
        """,
    )


# ---------------------------------------------------------------------------
# Parametrized quoting consistency across special identifier patterns
# ---------------------------------------------------------------------------

SPECIAL_IDENTIFIERS = [
    "my-table",
    "col with spaces",
    "123_starts_digit",
    "SELECT",
]


@pytest.mark.parametrize("identifier", SPECIAL_IDENTIFIERS)
def test_quote_default_handles_special_identifiers(identifier: str, data_source_test_helper: DataSourceTestHelper):
    """quote_default must return a quoted, non-None identifier for each special pattern."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    quoted = sql_dialect.quote_default(identifier)
    assert quoted is not None, f"quote_default returned None for '{identifier}'"
    assert quoted != identifier, f"quote_default returned bare identifier for '{identifier}'"


@pytest.mark.parametrize("identifier", SPECIAL_IDENTIFIERS)
def test_quote_for_ddl_handles_special_identifiers(identifier: str, data_source_test_helper: DataSourceTestHelper):
    """quote_for_ddl must return a quoted, non-None identifier for each special pattern."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    quoted = sql_dialect.quote_for_ddl(identifier)
    assert quoted is not None, f"quote_for_ddl returned None for '{identifier}'"
    assert quoted != identifier, f"quote_for_ddl returned bare identifier for '{identifier}'"


@pytest.mark.parametrize("identifier", SPECIAL_IDENTIFIERS)
def test_ddl_and_dml_quoting_both_preserve_identifier(identifier: str, data_source_test_helper: DataSourceTestHelper):
    """Both DDL and DML quoting must preserve the original identifier string."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    dml_quoted = sql_dialect.quote_default(identifier)
    ddl_quoted = sql_dialect.quote_for_ddl(identifier)
    assert identifier in dml_quoted, f"DML quoting lost identifier: {dml_quoted}"
    assert identifier in ddl_quoted, f"DDL quoting lost identifier: {ddl_quoted}"
