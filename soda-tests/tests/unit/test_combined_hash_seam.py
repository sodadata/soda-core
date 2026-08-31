"""The COMBINED_HASH operand seam: its shape, and that folding is opt-in.

`COMBINED_HASH` builds the key that identifies a row — `__soda_row_id`, the diagnostics-warehouse
keys, a duplicate check's failed-row groups. `comparison_mode="database-equality"` asks for values
the data source's own equality considers equal to hash alike, so a duplicate check's groups match
the verdict it reported. Only a dialect on a case- or accent-insensitive source has anything to do
about that, and it acts by overriding `_build_hash_operand`.

Two things are pinned here. First the shape, because every dialect inherits it and a key that
changes shape silently orphans every previously stored `__soda_row_id`. Second that the mode is
inert in the base and in every shipped dialect: it was added for MySQL, and a dialect picking it up
by accident would rewrite those keys.
"""

from __future__ import annotations

import pytest
from soda_core.common.sql_ast import COLUMN, COMBINED_HASH


@pytest.fixture
def postgres_dialect():
    # The shape is the base dialect's, but a bare SqlDialect() cannot render a CAST (it has no
    # type-name map), so it is pinned through the dialect that adds the least on top.
    module = pytest.importorskip("soda_postgres.common.data_sources.postgres_data_source")
    return module.PostgresSqlDialect()


def test_a_single_operand_is_hashed_without_a_separator(postgres_dialect):
    # No CONCAT_WS around one operand: a one-column key must hash the value itself, so it stays
    # comparable with a hash computed anywhere else over that column.
    sql = postgres_dialect.build_expression_sql(COMBINED_HASH([COLUMN("id")]))

    assert sql == """MD5(COALESCE("id"::varchar, '__SODA_NULL__'))"""


def test_several_operands_are_joined_by_the_pipe_separator(postgres_dialect):
    # The separator is what keeps ('a','bc') distinct from ('ab','c'); dropping it would collide
    # two different rows onto one key.
    sql = postgres_dialect.build_expression_sql(COMBINED_HASH([COLUMN("a"), COLUMN("b")]))

    assert sql == (
        """MD5(CONCAT_WS('||', COALESCE("a"::varchar, '__SODA_NULL__'), """
        """COALESCE("b"::varchar, '__SODA_NULL__')))"""
    )


def test_a_null_operand_falls_back_to_the_sentinel(postgres_dialect):
    # NULL must not swallow the whole CONCAT_WS-joined string, which is what an unguarded CAST
    # would do on most engines.
    sql = postgres_dialect.build_expression_sql(COMBINED_HASH([COLUMN("a")]))

    assert postgres_dialect.get_soda_null_string_value() in sql


# Every dialect class shipped in soda-core, including the two that are easy to miss because they
# are not one-per-package: soda-trino, and the Hive variant inside soda-databricks.
_DIALECT_CASES = [
    ("postgres", "soda_postgres.common.data_sources.postgres_data_source", "PostgresSqlDialect"),
    ("duckdb", "soda_duckdb.common.data_sources.duckdb_data_source", "DuckDBSqlDialect"),
    ("sqlserver", "soda_sqlserver.common.data_sources.sqlserver_data_source", "SqlServerSqlDialect"),
    ("snowflake", "soda_snowflake.common.data_sources.snowflake_data_source", "SnowflakeSqlDialect"),
    ("bigquery", "soda_bigquery.common.data_sources.bigquery_data_source", "BigQuerySqlDialect"),
    ("databricks", "soda_databricks.common.data_sources.databricks_data_source", "DatabricksSqlDialect"),
    ("databricks_hive", "soda_databricks.common.data_sources.databricks_data_source", "DatabricksHiveSqlDialect"),
    ("redshift", "soda_redshift.common.data_sources.redshift_data_source", "RedshiftSqlDialect"),
    ("athena", "soda_athena.common.data_sources.athena_data_source", "AthenaSqlDialect"),
    ("fabric", "soda_fabric.common.data_sources.fabric_data_source", "FabricSqlDialect"),
    ("synapse", "soda_synapse.common.data_sources.synapse_data_source", "SynapseSqlDialect"),
    ("sparkdf", "soda_sparkdf.common.data_sources.sparkdf_data_source", "SparkDataFrameSqlDialect"),
    ("trino", "soda_trino.common.data_sources.trino_data_source", "TrinoSqlDialect"),
]

# The exact SQL each dialect renders for a one-column and a two-column key, as (one, two).
#
# A regression snapshot, not a style guide. `__soda_row_id` and every diagnostics-warehouse key is
# one of these strings hashed, so a change here silently orphans previously stored identities: rows
# written before and after would no longer join. Nothing else in the suite would notice, because the
# SQL stays valid and self-consistent — the old and new hashes only disagree with each other.
#
# Captured from the rendering that predates `comparison_mode`, so these are literally the
# pre-flag strings. Some carry pre-existing quirks — BigQuery triple-quotes the sentinel and the
# separator, Redshift concatenates with `||` instead of CONCAT_WS — reproduced rather than corrected:
# this file's job is to detect drift, not to fix it. Changing one deliberately means migrating every
# stored key.
_EXPECTED_SQL = {
    "postgres": (
        "MD5(COALESCE(\"a\"::varchar, '__SODA_NULL__'))",
        "MD5(CONCAT_WS('||', COALESCE(\"a\"::varchar, '__SODA_NULL__'), " "COALESCE(\"b\"::varchar, '__SODA_NULL__')))",
    ),
    "duckdb": (
        "MD5(COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'))",
        "MD5(CONCAT_WS('||', COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'), "
        "COALESCE(CAST(\"b\" AS varchar), '__SODA_NULL__')))",
    ),
    "sqlserver": (
        "CONVERT(VARCHAR(32), HASHBYTES('MD5', COALESCE(CAST([a] AS varchar), '__SODA_NULL__')), 2)",
        "CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT_WS('||', COALESCE(CAST([a] AS varchar), "
        "'__SODA_NULL__'), COALESCE(CAST([b] AS varchar), '__SODA_NULL__'))), 2)",
    ),
    "snowflake": (
        "MD5(COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'))",
        "MD5(CONCAT_WS('||', COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'), "
        "COALESCE(CAST(\"b\" AS varchar), '__SODA_NULL__')))",
    ),
    "bigquery": (
        "to_hex(MD5(COALESCE(CAST(`a` AS string), '''__SODA_NULL__''')))",
        "to_hex(MD5(CONCAT(COALESCE(CAST(`a` AS string), '''__SODA_NULL__'''), ''||'', "
        "COALESCE(CAST(`b` AS string), '''__SODA_NULL__'''))))",
    ),
    "databricks": (
        "MD5(COALESCE(CAST(`a` AS string), '__SODA_NULL__'))",
        "MD5(CONCAT_WS('||', COALESCE(CAST(`a` AS string), '__SODA_NULL__'), "
        "COALESCE(CAST(`b` AS string), '__SODA_NULL__')))",
    ),
    "databricks_hive": (
        "MD5(COALESCE(CAST(`a` AS string), '__SODA_NULL__'))",
        "MD5(CONCAT_WS('||', COALESCE(CAST(`a` AS string), '__SODA_NULL__'), "
        "COALESCE(CAST(`b` AS string), '__SODA_NULL__')))",
    ),
    "redshift": (
        "MD5(COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'))",
        "MD5(COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__') || ''||'' || "
        "COALESCE(CAST(\"b\" AS varchar), '__SODA_NULL__'))",
    ),
    "athena": (
        "to_hex(md5(to_utf8(COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'))))",
        "to_hex(md5(to_utf8(CONCAT_WS('||', COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'), "
        "COALESCE(CAST(\"b\" AS varchar), '__SODA_NULL__')))))",
    ),
    "fabric": (
        "CONVERT(VARCHAR(32), HASHBYTES('MD5', COALESCE(CAST([a] AS varchar), '__SODA_NULL__')), 2)",
        "CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT_WS('||', COALESCE(CAST([a] AS varchar), "
        "'__SODA_NULL__'), COALESCE(CAST([b] AS varchar), '__SODA_NULL__'))), 2)",
    ),
    "synapse": (
        "CONVERT(VARCHAR(32), HASHBYTES('MD5', COALESCE(CAST([a] AS varchar), '__SODA_NULL__')), 2)",
        "CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT_WS('||', COALESCE(CAST([a] AS varchar), "
        "'__SODA_NULL__'), COALESCE(CAST([b] AS varchar), '__SODA_NULL__'))), 2)",
    ),
    "sparkdf": (
        "MD5(COALESCE(CAST(`a` AS string), '__SODA_NULL__'))",
        "MD5(CONCAT_WS('||', COALESCE(CAST(`a` AS string), '__SODA_NULL__'), "
        "COALESCE(CAST(`b` AS string), '__SODA_NULL__')))",
    ),
    "trino": (
        "TO_HEX(MD5(TO_UTF8(COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'))))",
        "TO_HEX(MD5(TO_UTF8(CONCAT_WS('||', COALESCE(CAST(\"a\" AS varchar), '__SODA_NULL__'), "
        "COALESCE(CAST(\"b\" AS varchar), '__SODA_NULL__')))))",
    ),
}


def _dialect(module_path: str, class_name: str):
    module = pytest.importorskip(module_path)
    return getattr(module, class_name)()


@pytest.mark.parametrize("name, module_path, class_name", _DIALECT_CASES, ids=[c[0] for c in _DIALECT_CASES])
def test_every_shipped_dialect_renders_the_pre_flag_sql(name, module_path, class_name):
    """The regression guard that comparing a dialect against itself cannot give.

    The same-dialect comparison below proves the flag is inert, but not that the shared builder
    still renders what it used to — refactoring it would change every dialect together and keep
    every such comparison green. These literals are the pre-flag output, so this is the assertion
    that would catch it.
    """
    sql_dialect = _dialect(module_path, class_name)
    expected_one, expected_two = _EXPECTED_SQL[name]

    assert sql_dialect.build_expression_sql(COMBINED_HASH([COLUMN("a")])) == expected_one
    assert sql_dialect.build_expression_sql(COMBINED_HASH([COLUMN("a"), COLUMN("b")])) == expected_two


def test_every_shipped_dialect_is_pinned():
    # A dialect added to _DIALECT_CASES without a snapshot would raise KeyError above, which reads
    # as a broken test rather than a missing pin. Say which is missing instead.
    assert {case[0] for case in _DIALECT_CASES} == set(_EXPECTED_SQL)


@pytest.mark.parametrize("name, module_path, class_name", _DIALECT_CASES, ids=[c[0] for c in _DIALECT_CASES])
@pytest.mark.parametrize("expressions", [[COLUMN("a")], [COLUMN("a"), COLUMN("b")]], ids=["one", "two"])
def test_folding_is_inert_in_every_shipped_dialect(name, module_path, class_name, expressions):
    """The other half: setting the flag must change nothing here.

    The snapshot above would also pass if a dialect started honouring the flag, because it only
    renders the default. This covers the opposite mistake — a dialect picking the flag up by
    accident, which would rewrite the row identities of a source that has nothing to fold.
    """
    sql_dialect = _dialect(module_path, class_name)

    assert sql_dialect.build_expression_sql(
        COMBINED_HASH(expressions, comparison_mode="database-equality")
    ) == sql_dialect.build_expression_sql(COMBINED_HASH(expressions))


class TestTheMultiColumnDuplicateVerdictFolds:
    """The verdict itself, which is the one place the flag decides PASS/FAIL rather than a sample.

    A single-column duplicate check counts with `COUNT(DISTINCT(column))`, so the data source's own
    text comparison decides — on a case- or accent-insensitive collation it folds. The multi-column
    check has no such column to count; it counts distinct COMBINED_HASH values instead, so unless
    the hash folds too the two check shapes answer differently about the same data, and the
    failed-rows query (which hashes the same way) cannot return the rows the count reported.

    Pinned at the metric because nothing downstream would notice: dropping the flag renders valid
    SQL, every structural assertion holds, and on a byte-exact data source the SQL is unchanged, so
    the whole suite stays green while a MySQL duplicate check silently changes its verdict.
    """

    def _sql_expression(self, column_expressions, check_filter=None):
        from soda_core.contracts.impl.check_types.duplicate_check import MultiColumnDistinctCountMetricImpl

        # Unbound against a stub: the real constructor needs a whole ContractImpl, and
        # sql_expression reads only these two attributes.
        metric = object.__new__(MultiColumnDistinctCountMetricImpl)
        metric.column_expressions = column_expressions
        metric.check_filter = check_filter
        return MultiColumnDistinctCountMetricImpl.sql_expression(metric)

    def _combined_hash(self, expression):
        """The COMBINED_HASH inside COUNT(DISTINCT(...)), however deeply it is wrapped."""
        from soda_core.common.sql_ast import COMBINED_HASH as HashNode

        found = []
        stack = [expression]
        while stack:
            node = stack.pop()
            if isinstance(node, HashNode):
                found.append(node)
            for attribute in vars(node).values() if hasattr(node, "__dict__") else []:
                if isinstance(attribute, list):
                    stack.extend(item for item in attribute if hasattr(item, "__dict__"))
                elif hasattr(attribute, "__dict__"):
                    stack.append(attribute)
        assert len(found) == 1, f"expected exactly one COMBINED_HASH, found {len(found)}"
        return found[0]

    def test_the_unfiltered_verdict_folds(self):
        expression = self._sql_expression([COLUMN("a"), COLUMN("b")])

        assert self._combined_hash(expression).comparison_mode == "database-equality"

    def test_the_filtered_verdict_folds_identically(self):
        # The CASE_WHEN branch is a separate construction and would be easy to update alone; a
        # check with a filter must not answer differently from one without.
        expression = self._sql_expression([COLUMN("a"), COLUMN("b")], check_filter="a IS NOT NULL")

        assert self._combined_hash(expression).comparison_mode == "database-equality"

    def test_the_verdict_and_the_failed_rows_query_group_alike(self):
        """Both sides of the seam, asserted together, on a dialect that honours the flag.

        This is the property that matters: the count and the rows it explains must come from the
        same grouping. soda-mysql is the only dialect that renders them differently, so it is the
        only one where the two can disagree.
        """
        pytest.importorskip("soda_mysql")
        from soda_mysql.common.data_sources.mysql_data_source import MysqlSqlDialect

        sql_dialect = MysqlSqlDialect()
        verdict_sql = sql_dialect.build_expression_sql(self._sql_expression([COLUMN("a"), COLUMN("b")]))

        assert "WEIGHT_STRING" in verdict_sql, (
            "the multi-column duplicate verdict must group the way the data source compares, or it "
            "reports a count whose rows the failed-rows query cannot return"
        )
