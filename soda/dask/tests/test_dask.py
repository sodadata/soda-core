from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.utils import execute_scan_and_get_scan_result


def test_dask_data_source_prefix(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
        checks for "{table_name}":
            - row_count > 0
        """,
    )
    assert scan_result["defaultDataSourceProperties"]["prefix"] == data_source_fixture.data_source_name
