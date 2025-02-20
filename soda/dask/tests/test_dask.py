from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.utils import execute_scan_and_get_scan_result


def test_dask_data_source():
    pass


def test_dask_row_count_with_duplicates(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
        checks for "{table_name}":
            - duplicate_percent(distance, country) < 0.5%
            - row_count > 0
        """,
    )
    assert scan_result["queries"][0]["sql"].upper().count("COUNT(*)") == 1
