from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_ast import COUNT, STAR
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_synapse.common.data_sources.synapse_data_source import SynapseSqlDialect


def test_random():
    sql_dialect: SynapseSqlDialect = SynapseSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0\nFROM [a];"


def test_count_renders_as_count_big():
    # Inherits the COUNT_BIG override from SqlServerSqlDialect — guards against accidental regressions.
    assert SynapseSqlDialect().build_expression_sql(COUNT(STAR())) == "COUNT_BIG(*)"


# ---------------------------------------------------------------------------
# Time-bucket + percentile seams. DATEDIFF/DATEADD and the typed DATETIME2
# literal are inherited from SqlServerSqlDialect; percentile is not available
# on Synapse dedicated SQL pools (APPROX_PERCENTILE_DISC's applies-to list
# excludes Synapse).
# ---------------------------------------------------------------------------


def test_supports_percentile_within_group_is_false():
    assert SynapseSqlDialect().supports_percentile_within_group() is False


def test_time_delta_inherits_sqlserver_datediff_form():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = SynapseSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr("[ts]"), "days", 1)
    )
    assert sql == "(DATEDIFF(second, '2020-06-20T00:00:00.000', ([ts])) / 86400)"


def test_add_interval_inherits_sqlserver_dateadd_form():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = SynapseSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "DATEADD(DAY, ((soda_partition__ + 1) * 1), '2020-06-20T00:00:00.000')"


def test_literal_timestamp_typed_inherits_datetime2_cast():
    from datetime import datetime

    assert (
        SynapseSqlDialect().literal_timestamp_typed(datetime(2020, 6, 20, 1, 2, 3))
        == "CAST('2020-06-20 01:02:03' AS DATETIME2)"
    )


# ---------------------------------------------------------------------------
# Paginated select — statement-level DISTINCT.
# Synapse dedicated SQL pools have no OFFSET/FETCH, so pagination goes through a
# ROW_NUMBER CTE. DISTINCT therefore cannot be a rewrite of the leading SELECT
# token (that is the inner, pre-ROW_NUMBER select) nor of the outer one (T-SQL
# rejects `SELECT DISTINCT ... ORDER BY <rn>` and rn must stay projected out) —
# it de-duplicates in an extra leading CTE that the paginating CTE then reads.
# ---------------------------------------------------------------------------


def _customers() -> DatasetIdentifier:
    return DatasetIdentifier(data_source_name="ds", prefixes=["soda_db", "dbo"], dataset_name="CUSTOMERS")


def test_select_all_paginated_sql_is_not_distinct_by_default():
    assert SynapseSqlDialect().select_all_paginated_sql(
        dataset_identifier=_customers(),
        columns=["id", "name"],
        filter="country = 'BE'",
        order_by=["id"],
        limit=10,
        offset=20,
    ) == (
        "WITH paginated AS (\n"
        "    SELECT [id], [name], ROW_NUMBER() OVER (ORDER BY [id] ASC) AS __soda_rn\n"
        "    FROM [soda_db].[dbo].[CUSTOMERS]\n"
        "    WHERE country = 'BE'\n"
        ")\n"
        "SELECT [id], [name]\n"
        "FROM paginated\n"
        "WHERE __soda_rn > 20 AND __soda_rn <= 30\n"
        "ORDER BY __soda_rn;"
    )


def test_select_all_paginated_sql_distinct_deduplicates_before_row_number():
    assert SynapseSqlDialect().select_all_paginated_sql(
        dataset_identifier=_customers(),
        columns=["id", "name"],
        filter="country = 'BE'",
        order_by=["id"],
        limit=10,
        offset=20,
        distinct=True,
    ) == (
        "WITH deduplicated AS (\n"
        "    SELECT DISTINCT [id], [name]\n"
        "    FROM [soda_db].[dbo].[CUSTOMERS]\n"
        "    WHERE country = 'BE'\n"
        "),\n"
        "paginated AS (\n"
        "    SELECT [id], [name], ROW_NUMBER() OVER (ORDER BY [id] ASC) AS __soda_rn\n"
        "    FROM deduplicated\n"
        ")\n"
        "SELECT [id], [name]\n"
        "FROM paginated\n"
        "WHERE __soda_rn > 20 AND __soda_rn <= 30\n"
        "ORDER BY __soda_rn;"
    )


def test_select_all_paginated_sql_distinct_without_filter_or_order_by():
    assert SynapseSqlDialect().select_all_paginated_sql(
        dataset_identifier=_customers(),
        columns=["id"],
        filter=None,
        order_by=[],
        limit=5,
        offset=0,
        distinct=True,
    ) == (
        "WITH deduplicated AS (\n"
        "    SELECT DISTINCT [id]\n"
        "    FROM [soda_db].[dbo].[CUSTOMERS]\n"
        "    \n"
        "),\n"
        "paginated AS (\n"
        "    SELECT [id], ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS __soda_rn\n"
        "    FROM deduplicated\n"
        ")\n"
        "SELECT [id]\n"
        "FROM paginated\n"
        "WHERE __soda_rn > 0 AND __soda_rn <= 5\n"
        "ORDER BY __soda_rn;"
    )
