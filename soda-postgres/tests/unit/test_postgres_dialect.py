import pytest
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_ast import EQ, LIMIT, LITERAL, OFFSET, WHERE
from soda_core.common.sql_dialect import COLUMN, FROM, RANDOM, REGEX_LIKE, SELECT, STAR, SamplerType
from soda_postgres.common.data_sources.postgres_data_source import PostgresSqlDialect


@pytest.mark.parametrize(
    "sql_ast, expected_sql",
    [
        pytest.param(
            [SELECT(STAR()), FROM("a").SAMPLE(SamplerType.PERCENTAGE, 10)],
            'SELECT *\nFROM "a" TABLESAMPLE BERNOULLI(10);',
            id="tablesample_percentage(10%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("b").SAMPLE(SamplerType.PERCENTAGE, 25)],
            'SELECT *\nFROM "b" TABLESAMPLE BERNOULLI(25);',
            id="tablesample_percentage(25%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("c").SAMPLE(SamplerType.PERCENTAGE, 100)],
            'SELECT *\nFROM "c" TABLESAMPLE BERNOULLI(100);',
            id="tablesample_percentage(100%)",
        ),
    ],
)
def test_tablesample(sql_ast, expected_sql):
    sql_dialect: PostgresSqlDialect = PostgresSqlDialect()
    assert sql_dialect.build_select_sql(sql_ast) == expected_sql


@pytest.mark.parametrize(
    "sql_ast, expected_exception_message",
    [
        pytest.param(
            [SELECT(STAR()), FROM("customers").SAMPLE(SamplerType.ABSOLUTE_LIMIT, 100)],
            "Unsupported sampler type: ABSOLUTE_LIMIT",
            id="tablesample_not_supported_absolute_limit",
        ),
    ],
)
def test_tablesample_not_supported(sql_ast, expected_exception_message):
    sql_dialect: PostgresSqlDialect = PostgresSqlDialect()

    with pytest.raises(ValueError) as ex:
        sql_dialect.build_select_sql(sql_ast)

    assert str(ex.value) == expected_exception_message


def test_random():
    sql_dialect: PostgresSqlDialect = PostgresSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'


@pytest.mark.parametrize(
    "schema_name, expected",
    [
        ("pg_catalog", True),
        ("pg_toast", True),
        ("pg_temp_1", True),
        ("information_schema", True),
        ("PG_CATALOG", True),
        ("INFORMATION_SCHEMA", True),
        ("public", False),
        ("pguser", False),
    ],
)
def test_is_system_schema(schema_name, expected):
    assert PostgresSqlDialect().is_system_schema(schema_name) is expected


def test_primary_keys_query_reads_pg_catalog_not_information_schema():
    """The PK query must read pg_catalog: information_schema.table_constraints /
    key_column_usage are filtered to tables the current user owns or holds a
    non-SELECT privilege on, so a read-only monitoring user silently gets zero
    primary keys through them."""
    from soda_postgres.statements.postgres_metadata_primary_keys_query import PostgresMetadataPrimaryKeysQuery

    dialect = PostgresSqlDialect()
    query = PostgresMetadataPrimaryKeysQuery(sql_dialect=dialect, data_source_connection=None)
    namespace = query._build_namespace(["soda_test", "MySchema"])
    sql = dialect.build_select_sql(query.build_sql_statement(namespace, ["orders"]))

    assert "information_schema" not in sql
    assert '"pg_catalog"."pg_constraint"' in sql
    assert '"pg_catalog"."pg_attribute"' in sql
    # conkey position keeps composite keys in declared order.
    assert "array_position(constraints.conkey, key_columns.attnum)" in sql
    # Schema filter is case-insensitive (mirrors PostgresMetadataTablesQuery), so a schema
    # spelled in a different case doesn't silently match nothing.
    assert 'LOWER("schemas"."nspname") = \'myschema\'' in sql


def test_regex_like_pattern_goes_through_literal_string():
    """PostgresSqlDialect overrides _build_regex_like_sql (it renders `~`), so it needs
    its own copy of the base-dialect guarantee. Postgres literals are standard
    conforming, so the backslash passes through unchanged -- but an apostrophe in a
    user pattern still breaks the query unless it is escaped. See SCS-1413.
    """
    sql_dialect = PostgresSqlDialect()
    assert sql_dialect.build_expression_sql(REGEX_LIKE(COLUMN("c"), r"^1\.5$")) == "\"c\" ~ '^1\\.5$'"
    assert sql_dialect.build_expression_sql(REGEX_LIKE(COLUMN("c"), "^it's$")) == "\"c\" ~ '^it''s$'"


def test_pagination_statements_render_the_base_trailing_clause():
    sql_dialect = PostgresSqlDialect()

    elements = sql_dialect.pagination_statements(limit=100, offset=200)

    assert [type(element) for element in elements] == [LIMIT, OFFSET]
    assert sql_dialect.build_select_sql(elements, add_semicolon=False) == "LIMIT 100\nOFFSET 200"


def test_select_all_paginated_sql_composes_with_pagination_statements():
    sql = PostgresSqlDialect().select_all_paginated_sql(
        dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=["public"], dataset_name="orders"),
        columns=["id", "name"],
        filter=None,
        order_by=["id"],
        limit=100,
        offset=200,
    )

    assert sql == 'SELECT "id",\n       "name"\nFROM "public"."orders"\nORDER BY "id" ASC\nLIMIT 100\nOFFSET 200;'


def test_pagination_clause_sql_renders_order_by_and_the_window():
    sql_dialect = PostgresSqlDialect()

    clause = sql_dialect.pagination_clause_sql(order_by=["id"], limit=100, offset=200)

    assert clause == 'ORDER BY "id" ASC\nLIMIT 100\nOFFSET 200'


def test_pagination_clause_sql_folds_normalized_keys_with_a_tiebreaker():
    """The clause orders through `_order_by_key`, so a normalized text key gets the same
    LOWER() fold plus raw-column tiebreaker the generated select orders by -- the two
    sides of one merge join must sort identically."""
    sql_dialect = PostgresSqlDialect()

    clause = sql_dialect.pagination_clause_sql(
        order_by=["k"], limit=10, offset=0, normalize_key_columns=frozenset({"k"})
    )

    assert clause == 'ORDER BY LOWER("k") ASC, "k" ASC\nLIMIT 10\nOFFSET 0'


def test_pagination_clause_sql_is_none_for_a_wrapping_paginator():
    class _WrappingPaginatorDialect(PostgresSqlDialect, sqlglot_dialect="postgres"):
        def pagination_statements(self, limit: int, offset: int):
            return None

    assert _WrappingPaginatorDialect().pagination_clause_sql(order_by=["id"], limit=1, offset=0) is None


def test_a_dialect_declaring_no_trailing_pagination_must_own_its_paginated_select():
    """`pagination_statements() -> None` declares a wrapping paginator (Synapse); the base
    `select_all_paginated_sql` cannot render for such a dialect and says so instead of
    emitting a trailing clause the engine would reject."""

    class _NoTrailingPaginationDialect(PostgresSqlDialect, sqlglot_dialect="postgres"):
        def pagination_statements(self, limit: int, offset: int):
            return None

    with pytest.raises(ValueError, match="pagination_statements"):
        _NoTrailingPaginationDialect().select_all_paginated_sql(
            dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=[], dataset_name="orders"),
            columns=["id"],
            filter=None,
            order_by=["id"],
            limit=1,
            offset=0,
        )


# ---------------------------------------------------------------------------
# FROM-less SELECT — `FROM_LESS_SELECT_TABLE`
# ---------------------------------------------------------------------------


class _FromLessRejectingDialect(PostgresSqlDialect, sqlglot_dialect="postgres"):
    """Stands in for HANA / Db2 / Oracle: every SELECT must carry a FROM clause."""

    FROM_LESS_SELECT_TABLE = "SYS.DUMMY"


def test_a_from_less_select_carries_no_from_line_by_default():
    """Postgres accepts `SELECT 1`, so the base declares no table and emits no FROM line.
    Pinned because a dangling `FROM ` here is what dialects used to patch out of the
    rendered text."""
    assert PostgresSqlDialect().build_select_sql([SELECT([LITERAL(1)])]) == "SELECT 1;"


def test_a_declaring_dialect_substitutes_its_one_row_table():
    assert _FromLessRejectingDialect().build_select_sql([SELECT([LITERAL(1)])]) == "SELECT 1\nFROM SYS.DUMMY;"


def test_the_substituted_table_lands_in_the_from_slot_before_trailing_clauses():
    """The clause is rendered in the base composition, so every clause emitted after the
    FROM slot still follows it. Appending the table to the finished statement instead put
    it after the WHERE."""
    sql = _FromLessRejectingDialect().build_select_sql([SELECT([LITERAL(1)]), WHERE(EQ(LITERAL(1), LITERAL(1)))])

    assert sql == "SELECT 1\nFROM SYS.DUMMY\nWHERE 1 = 1;"


def test_a_real_from_element_is_left_alone_on_a_declaring_dialect():
    sql = _FromLessRejectingDialect().build_select_sql([SELECT([COLUMN("a")]), FROM("t")])

    assert sql == 'SELECT "a"\nFROM "t";'


def test_a_clause_only_element_list_gets_no_table_on_a_declaring_dialect():
    """`pagination_statements` renders through `build_select_sql` with no SELECT element.
    That is a clause, not a FROM-less SELECT, and must not acquire a FROM."""
    sql_dialect = _FromLessRejectingDialect()

    elements = sql_dialect.pagination_statements(limit=100, offset=200)

    assert sql_dialect.build_select_sql(elements, add_semicolon=False) == "LIMIT 100\nOFFSET 200"
