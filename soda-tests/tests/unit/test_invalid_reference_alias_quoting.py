"""The referencing alias in a valid_reference_data JOIN is quoted by the dialect.

`InvalidReferenceCountQuery.query_join` rewrites a `column_expression` so the column it names is
qualified with the join alias. It used to hardcode a double quote — `f'"{alias}".{column}'` — which
is only correct for the dialects whose identifier quote character happens to be `"`.

On BigQuery and Databricks a double-quoted identifier is a STRING LITERAL, so `"C".country` is a
syntax error rather than a qualified column: any contract combining a `column_expression` with
`valid_reference_data` could not run at all. On the SQL Server family the alias needs brackets.

This is the coverage that was missing. The only existing test that reaches this branch is
soda-tests/tests/feature/test_column_expression.py, and the feature suite runs on the postgres lane
only — the one dialect whose output the change does NOT alter. So the fix shipped exercised on
exactly zero of the dialects it affects.

Note on where this runs: soda-tests/tests/unit is itself collected on the postgres lane only
(soda-core's matrix) and on no soda-extensions lane, so these do not run "everywhere" either. What
they do give is coverage that is independent of a live server and of which dialect the lane targets,
so the bigquery/databricks/sqlserver rendering is asserted at all — which it previously was not.
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from soda_core.common.sql_ast import SqlExpressionStr
from soda_core.contracts.impl.check_types.invalidity_check import InvalidReferenceCountQuery

# One representative per quoting style, plus the dialects that share it.
_DIALECT_CASES = [
    ("postgres", "soda_postgres.common.data_sources.postgres_data_source", "PostgresSqlDialect", '"C"'),
    ("bigquery", "soda_bigquery.common.data_sources.bigquery_data_source", "BigQuerySqlDialect", "`C`"),
    ("databricks", "soda_databricks.common.data_sources.databricks_data_source", "DatabricksSqlDialect", "`C`"),
    ("sqlserver", "soda_sqlserver.common.data_sources.sqlserver_data_source", "SqlServerSqlDialect", "[C]"),
    ("duckdb", "soda_duckdb.common.data_sources.duckdb_data_source", "DuckDBSqlDialect", '"C"'),
]


def _dialect(module_path: str, class_name: str):
    module = pytest.importorskip(module_path)
    return getattr(module, class_name)()


def _query_join_sql(sql_dialect, column_expression: str, column_name: str = "country") -> str:
    """Drive the real query_join against a stub and return the rendered JOIN clause.

    Unbound-method-against-a-stub, the pattern test_rows_diff_key_parts.py already uses: the real
    class needs a whole ContractImpl to construct, and re-implementing the rewrite here would test
    a copy rather than the code that ships.
    """
    stub = Mock()
    stub.referencing_alias = "C"
    stub.referenced_alias = "R"
    stub._referenced_cte_name = "REF"
    stub.data_source_impl.sql_dialect = sql_dialect
    stub.metric_impl.column_impl.column_yaml.name = column_name
    stub.metric_impl.column_expression = SqlExpressionStr(column_expression)
    stub.metric_impl.missing_and_validity.valid_reference_data.column = "code"
    # The two predicates are not what this test is about; make them render as something inert so
    # the JOIN clause is what the assertion sees.
    stub.metric_impl.missing_and_validity.is_missing_expr = lambda expr: SqlExpressionStr("1=0")
    stub.metric_impl.missing_and_validity.is_invalid_expr = lambda expr: SqlExpressionStr("1=1")

    clauses = InvalidReferenceCountQuery.query_join(stub)
    # build_select_sql renders whole clauses (JOIN / WHERE); build_expression_sql only handles
    # expression nodes and refuses a clause.
    return sql_dialect.build_select_sql(clauses)


class TestTheAliasIsQuotedByTheDialect:
    @pytest.mark.parametrize(
        "name, module_path, class_name, expected_alias",
        _DIALECT_CASES,
        ids=[case[0] for case in _DIALECT_CASES],
    )
    def test_the_alias_uses_the_dialects_own_quote_character(self, name, module_path, class_name, expected_alias):
        sql = _query_join_sql(_dialect(module_path, class_name), "country::json->>'country_code'")

        assert f"{expected_alias}.country" in sql, sql

    @pytest.mark.parametrize(
        "name, module_path, class_name, expected_alias",
        _DIALECT_CASES,
        ids=[case[0] for case in _DIALECT_CASES],
    )
    def test_a_double_quoted_alias_is_not_emitted_for_a_non_double_quote_dialect(
        self, name, module_path, class_name, expected_alias
    ):
        """The regression itself: `"C".country` on a backtick or bracket dialect.

        Asserted as an absence rather than only as a presence, because the previous
        implementation emitted BOTH the right column and the wrong quoting.
        """
        sql = _query_join_sql(_dialect(module_path, class_name), "country::json->>'country_code'")

        if expected_alias != '"C"':
            assert '"C".' not in sql, sql

    def test_the_word_boundary_rewrite_still_only_replaces_the_standalone_column(self):
        # Guards the pre-existing behaviour the quoting change sits inside: `country_code` inside
        # the JSON path must not be qualified, only the standalone `country`.
        sql = _query_join_sql(_dialect(_DIALECT_CASES[1][1], _DIALECT_CASES[1][2]), "country::json->>'country_code'")

        assert "`C`.country::json->>'country_code'" in sql, sql
        assert "`C`.country_code" not in sql, sql

    def test_a_plain_column_expression_is_left_to_the_ast(self):
        # Only a SqlExpressionStr takes the rewrite path; a COLUMN is qualified by the AST's own
        # .IN(alias), so the string substitution must not be involved at all.
        sql_dialect = _dialect(_DIALECT_CASES[1][1], _DIALECT_CASES[1][2])
        stub = Mock()
        stub.referencing_alias = "C"
        stub.referenced_alias = "R"
        stub._referenced_cte_name = "REF"
        stub.data_source_impl.sql_dialect = sql_dialect
        stub.metric_impl.column_impl.column_yaml.name = "country"
        from soda_core.common.sql_ast import COLUMN

        stub.metric_impl.column_expression = COLUMN("country")
        stub.metric_impl.missing_and_validity.valid_reference_data.column = "code"
        stub.metric_impl.missing_and_validity.is_missing_expr = lambda expr: SqlExpressionStr("1=0")
        stub.metric_impl.missing_and_validity.is_invalid_expr = lambda expr: SqlExpressionStr("1=1")

        clauses = InvalidReferenceCountQuery.query_join(stub)
        sql = sql_dialect.build_select_sql(clauses)

        assert "`C`.`country`" in sql, sql
