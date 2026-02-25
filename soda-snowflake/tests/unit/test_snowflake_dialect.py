import pytest
from soda_core.common.sql_dialect import FROM, SELECT, STAR, SamplerType
from soda_snowflake.common.data_sources.snowflake_data_source import SnowflakeSqlDialect


@pytest.mark.parametrize(
    "sql_ast, expected_sql",
    [
        pytest.param(
            [SELECT(STAR()), FROM("a").SAMPLE(SamplerType.PERCENTAGE, 10)],
            'SELECT *\nFROM "a" TABLESAMPLE (10);',
            id="tablesample_percentage(10%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("b").SAMPLE(SamplerType.PERCENTAGE, 25)],
            'SELECT *\nFROM "b" TABLESAMPLE (25);',
            id="tablesample_percentage(25%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("c").SAMPLE(SamplerType.PERCENTAGE, 100)],
            'SELECT *\nFROM "c" TABLESAMPLE (100);',
            id="tablesample_percentage(100%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("d").SAMPLE(SamplerType.ABSOLUTE_LIMIT, 50)],
            'SELECT *\nFROM "d" TABLESAMPLE (50 ROWS);',
            id="tablesample_absolute_limit(50)",
        ),
    ],
)
def test_tablesample(sql_ast, expected_sql):
    sql_dialect: SnowflakeSqlDialect = SnowflakeSqlDialect()
    assert sql_dialect.build_select_sql(sql_ast) == expected_sql
