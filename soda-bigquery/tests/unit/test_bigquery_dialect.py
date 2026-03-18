import pytest
from soda_bigquery.common.data_sources.bigquery_data_source import BigQuerySqlDialect
from soda_core.common.sql_dialect import FROM, SELECT, STAR, SamplerType


@pytest.mark.parametrize(
    "sql_ast, expected_sql",
    [
        pytest.param(
            [SELECT(STAR()), FROM("a").SAMPLE(SamplerType.PERCENTAGE, 10)],
            "SELECT *\nFROM `a` TABLESAMPLE SYSTEM (10 PERCENT);",
            id="tablesample_percentage(10%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("b").SAMPLE(SamplerType.PERCENTAGE, 25)],
            "SELECT *\nFROM `b` TABLESAMPLE SYSTEM (25 PERCENT);",
            id="tablesample_percentage(25%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("c").SAMPLE(SamplerType.PERCENTAGE, 100)],
            "SELECT *\nFROM `c` TABLESAMPLE SYSTEM (100 PERCENT);",
            id="tablesample_percentage(100%)",
        ),
    ],
)
def test_tablesample(sql_ast, expected_sql):
    sql_dialect = BigQuerySqlDialect()
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
    sql_dialect = BigQuerySqlDialect()

    with pytest.raises(ValueError) as ex:
        sql_dialect.build_select_sql(sql_ast)

    assert str(ex.value) == expected_exception_message
