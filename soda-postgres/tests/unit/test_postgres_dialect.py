import pytest
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
