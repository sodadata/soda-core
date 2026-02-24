import pytest
from soda_core.common.sql_dialect import FROM, SELECT, STAR, SamplerType
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
        pytest.param(
            [SELECT(STAR()), FROM("customers").SAMPLE(SamplerType.PERCENTAGE, -10)],
            "Sample size for percentage sampler type must be between 0 and 100, but got -10",
            id="tablesample_percentage_invalid_negative",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("customers").SAMPLE(SamplerType.PERCENTAGE, 110)],
            "Sample size for percentage sampler type must be between 0 and 100, but got 110",
            id="tablesample_percentage_invalid_positive",
        ),
    ],
)
def test_tablesample_not_supported(sql_ast, expected_exception_message):
    sql_dialect: PostgresSqlDialect = PostgresSqlDialect()

    with pytest.raises(ValueError) as ex:
        sql_dialect.build_select_sql(sql_ast)

    assert str(ex.value) == expected_exception_message
