import pytest
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.metadata_types import SqlDataType
from soda_core.common.sql_ast import COUNT, CREATE_TABLE, CREATE_TABLE_COLUMN, STAR
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_synapse.common.data_sources.synapse_data_source import SynapseSqlDialect


def test_random():
    sql_dialect: SynapseSqlDialect = SynapseSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0\nFROM [a];"


def test_primary_key_create_table_is_nonclustered_not_enforced_with_not_null_columns():
    # Synapse only accepts a NONCLUSTERED ... NOT ENFORCED primary key, and its columns
    # must be NOT NULL. The custom HEAP create-table builder still emits the PK clause.
    dialect = SynapseSqlDialect()
    create_table = CREATE_TABLE(
        fully_qualified_table_name="db.dbo.orders",
        columns=[
            CREATE_TABLE_COLUMN(name="tenant_id", type=SqlDataType("int")),
            CREATE_TABLE_COLUMN(name="id", type=SqlDataType("int")),
            CREATE_TABLE_COLUMN(name="label", type=SqlDataType("varchar", character_maximum_length=10)),
        ],
        primary_key_column_names=["tenant_id", "id"],
    )
    sql = dialect.build_create_table_sql(create_table)
    assert "[tenant_id] int NOT NULL" in sql
    assert "[id] int NOT NULL" in sql
    assert "PRIMARY KEY NONCLUSTERED ([tenant_id], [id]) NOT ENFORCED" in sql
    assert sql.rstrip(";").endswith("WITH (HEAP)")


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


# ---------------------------------------------------------------------------
# select_all_paginated_sql — Synapse has no OFFSET/FETCH, so it hand-rolls a
# ROW_NUMBER() window fold. This whole branch was previously unexercised; pin
# the rn window, the LOWER() key-normalization seam (default-off), the
# empty-order_by fallback, and the fold combined with DISTINCT.
# ---------------------------------------------------------------------------


def _paginated(order_by, normalize_key_columns=frozenset(), distinct=False):
    from soda_core.common.dataset_identifier import DatasetIdentifier

    return SynapseSqlDialect().select_all_paginated_sql(
        dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=["s"], dataset_name="t"),
        columns=["code", "label"],  # explicit → no get_column_names resolver needed
        filter=None,
        order_by=order_by,
        limit=10,
        offset=20,
        normalize_key_columns=normalize_key_columns,
        distinct=distinct,
    )


def test_paginated_row_number_fold_windows_and_default_off():
    sql = _paginated(order_by=["code"])
    assert "ROW_NUMBER() OVER (ORDER BY [code] ASC) AS __soda_rn" in sql
    assert "WHERE __soda_rn > 20 AND __soda_rn <= 30" in sql
    assert "LOWER" not in sql.upper()  # default-off → no case-fold


def test_paginated_row_number_fold_normalizes_only_flagged_key():
    sql = _paginated(order_by=["code", "label"], normalize_key_columns=frozenset({"code"}))
    # LOWER fold + raw tiebreaker so the ROW_NUMBER window order is total (deterministic paging);
    # the co-ordered non-flagged "label" stays raw.
    assert "LOWER([code]) ASC, [code] ASC" in sql
    assert sql.upper().count("LOWER(") == 1


def test_paginated_normalized_fold_escapes_bracket_in_identifier():
    # Regression: the LOWER() case-fold must escape an embedded `]` the same way the raw tiebreaker
    # does. This paginator hand-builds SQL, so a column name containing `]` would otherwise break
    # out of the `[...]` quoting (T-SQL injection) via the AST's non-escaping `quote_default`.
    from soda_core.common.dataset_identifier import DatasetIdentifier

    sql = SynapseSqlDialect().select_all_paginated_sql(
        dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=["s"], dataset_name="t"),
        columns=["a]b"],
        filter=None,
        order_by=["a]b"],
        limit=10,
        offset=20,
        normalize_key_columns=frozenset({"a]b"}),
    )
    assert "LOWER([a]]b]) ASC, [a]]b] ASC" in sql  # both halves double the `]`
    assert "LOWER([a]b])" not in sql  # the unescaped form must never appear


def test_paginated_empty_order_by_falls_back_to_select_null():
    assert "ORDER BY (SELECT NULL)" in _paginated(order_by=[])


def test_paginated_distinct_with_normalized_key_folds_inside_the_row_number_window():
    """DISTINCT and the case-fold compose without an extra shape here: the de-duplication already
    happens in its own CTE, and the ROW_NUMBER select over it is not DISTINCT, so T-SQL accepts
    LOWER([code]) in the window's ORDER BY."""
    sql = _paginated(order_by=["code"], normalize_key_columns=frozenset({"code"}), distinct=True)
    assert sql == (
        "WITH deduplicated AS (\n"
        "    SELECT DISTINCT [code], [label]\n"
        "    FROM [s].[t]\n"
        "    \n"
        "),\n"
        "paginated AS (\n"
        "    SELECT [code], [label], ROW_NUMBER() OVER (ORDER BY LOWER([code]) ASC, [code] ASC) AS __soda_rn\n"
        "    FROM deduplicated\n"
        ")\n"
        "SELECT [code], [label]\n"
        "FROM paginated\n"
        "WHERE __soda_rn > 20 AND __soda_rn <= 30\n"
        "ORDER BY __soda_rn;"
    )


def test_paginated_distinct_rejects_an_unprojected_order_by_column():
    """The `deduplicated` CTE projects only `columns`, so ordering by anything else can't resolve.
    Refuse it here — like the rest of this dialect refuses what it cannot express — rather than
    emit SQL Synapse rejects with "Invalid column name"."""
    with pytest.raises(ValueError, match=r"can only order by projected columns"):
        _paginated(order_by=["other"], distinct=True)


def test_paginated_distinct_rejects_an_unprojected_normalized_key():
    with pytest.raises(ValueError, match=r"can only order by projected columns"):
        _paginated(order_by=["other"], normalize_key_columns=frozenset({"other"}), distinct=True)
