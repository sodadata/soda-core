from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import FROM, RANDOM, SELECT
from soda_core.common.sql_dialect import SqlDialect

NUM_ROWS = 10

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("testing_random")
    .column_integer("id")
    .rows(rows=[(i,) for i in range(NUM_ROWS)])
    .build()
)


def test_random(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    my_table_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)

    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect

    select_sql = sql_dialect.build_select_sql(
        [
            SELECT(RANDOM()),
            FROM(sql_dialect.get_from_name_from_qualified_name(my_table_name)),
        ]
    )
    result: QueryResult = data_source_impl.execute_query(select_sql)

    assert len(result.rows) == NUM_ROWS
    values = [float(row[0]) for row in result.rows]
    for value in values:
        assert 0.0 <= value < 1.0, f"RANDOM() returned {value}, expected a value in [0.0, 1.0)"
    assert len(set(values)) == NUM_ROWS, f"Expected {NUM_ROWS} distinct values, got {values}"
