from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import RANDOM
from soda_core.common.sql_dialect import SqlDialect


def test_random(data_source_test_helper: DataSourceTestHelper):
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect

    random_sql = sql_dialect._build_random_sql(RANDOM())
    select_sql = f"SELECT {random_sql}, {random_sql}, {random_sql}"
    result: QueryResult = data_source_impl.execute_query(select_sql)

    assert len(result.rows) == 1
    values = [float(result.rows[0][i]) for i in range(3)]
    for value in values:
        assert 0.0 <= value < 1.0, f"RANDOM() returned {value}, expected a value in [0.0, 1.0)"
    assert len(set(values)) == 3, f"Expected 3 distinct values, got {values}"
