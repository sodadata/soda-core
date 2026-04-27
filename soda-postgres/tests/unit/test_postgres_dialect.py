import pytest
from soda_core.common.sql_dialect import (
    COUNT,
    DISTINCT,
    EQ,
    FROM,
    LIMIT,
    OFFSET,
    ORDER_BY_ASC,
    RANDOM,
    SELECT,
    STAR,
    WHERE,
    SamplerType,
)
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
    "sql_ast, expected_sql",
    [
        pytest.param(
            [SELECT(fields=["a"], distinct=True), FROM("t")],
            'SELECT DISTINCT "a"\nFROM "t";',
            id="select_distinct_single_column",
        ),
        pytest.param(
            [SELECT(fields=["a", "b", "c"], distinct=True), FROM("t")],
            'SELECT DISTINCT "a",\n       "b",\n       "c"\nFROM "t";',
            id="select_distinct_multiple_columns_no_parens",
        ),
        pytest.param(
            [SELECT(fields=["a", "b"]), FROM("t")],
            'SELECT "a",\n       "b"\nFROM "t";',
            id="select_without_distinct_unchanged",
        ),
        pytest.param(
            [
                SELECT(fields=["a", "b"], distinct=True),
                FROM("t"),
                WHERE(EQ("status", 1)),
                ORDER_BY_ASC("a"),
                LIMIT(100),
                OFFSET(200),
            ],
            (
                'SELECT DISTINCT "a",\n'
                '       "b"\n'
                'FROM "t"\n'
                'WHERE "status" = 1\n'
                'ORDER BY "a" ASC\n'
                "LIMIT 100\n"
                "OFFSET 200;"
            ),
            id="select_distinct_paginated_full_shape",
        ),
        pytest.param(
            [SELECT(fields=COUNT(DISTINCT(expression="x"))), FROM("t")],
            'SELECT COUNT(DISTINCT("x"))\nFROM "t";',
            id="aggregate_level_distinct_preserves_parens",
        ),
    ],
)
def test_select_distinct(sql_ast, expected_sql):
    sql_dialect: PostgresSqlDialect = PostgresSqlDialect()
    assert sql_dialect.build_select_sql(sql_ast) == expected_sql


def test_select_distinct_default_is_false():
    # Backwards compatibility: omitting distinct must render SELECT (not SELECT DISTINCT).
    sql_dialect: PostgresSqlDialect = PostgresSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(["a"]), FROM("t")])
    assert sql == 'SELECT "a"\nFROM "t";'
