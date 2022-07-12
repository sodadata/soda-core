import pytest
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="postgres-only aggregation functions",
)
def test_data_source_specific_statistics_aggregation_metrics(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    all_checks = {
        "stdev": "stddev(size) between 3.26 and 3.27",
        "stddev_pop": "stddev_pop(size) between 3.02 and 3.03",
        "stddev_samp": "stddev_samp(size) between 3.26 and 3.27",
        "variance(size)": "variance(size) between 10.65 and 10.66",
        "var_pop(size)": "var_pop(size) between 9.13 and 9.14",
        "var_samp(size)": "var_samp(size) between 10.65 and 10.66",
        "percentile(distance, 0.7)": "percentile(distance, 0.7) = 999",
    }

    supported_checks = all_checks

    if test_data_source in ["bigquery", "redshift", "athena"]:
        supported_checks.pop("percentile(distance, 0.7)")

    checks_str = ""
    for check in supported_checks.values():
        checks_str += f"  - {check}\n"

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
checks for {table_name}:
{checks_str}
configurations for {table_name}:
  valid min for distance: 0
"""
    )
    scan.execute()

    scan.assert_all_checks_pass()
