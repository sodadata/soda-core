from textwrap import dedent

import pytest
from helpers.common_test_tables import (
    customers_dist_check_test_table,
    customers_test_table,
)
from helpers.data_source_fixture import DataSourceFixture


# TODO: valid_count does not seem to produce implicit samples, investigate.
@pytest.mark.parametrize(
    "check, skip_samples",
    [
        pytest.param("- missing_count(cat) = 0", False, id="missing_count"),
        pytest.param("- missing_percent(cat) = 0", False, id="missing_percent"),
        pytest.param(
            """- invalid_count(cat) = 0:
                   valid format: email""",
            False,
            id="invalid_count",
        ),
        pytest.param(
            """- invalid_percent(cat) = 0:
                   valid format: email""",
            False,
            id="invalid_percent",
        ),
        pytest.param("- duplicate_count(cat) = 0", True, id="duplicate_count"),
        pytest.param("- duplicate_percent(cat) = 0", True, id="duplicate_percent"),
        pytest.param("- values in (cst_size) must exist in {{another_table_name}} (cst_size)", False, id="reference"),
        pytest.param(
            """- failed rows:
                   fail condition: cat = 'HIGH' and cst_size < .7""",
            False,
            id="failed_rows_condition",
        ),
        pytest.param(
            """- failed rows:
                   fail query: Select * from sodatest_customers_6c2f3574 WHERE cat = 'HIGH' and cst_size < .7""",
            True,
            id="failed_rows_expression",
        ),
    ],
)
def test_dataset_checks(check: str, skip_samples: bool, data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    another_table_name = data_source_fixture.ensure_test_table(customers_dist_check_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()

    scan._configuration.exclude_columns = {table_name: ["cat", "cst_size"]}

    if "{{another_table_name}}" in check:
        check = check.replace("{{another_table_name}}", another_table_name)

    scan.add_sodacl_yaml_str(
        dedent(
            f"""
          checks for {table_name}:
            {check}
        """
        )
    )
    scan.execute()

    scan.assert_no_error_nor_warning_logs()
    if skip_samples:
        scan.assert_log_info("Skipping samples from query")
        assert "failedRowsFile" not in mock_soda_cloud.find_check_diagnostics(0).keys()
    else:
        scan.assert_no_log("Skipping samples from query")
        assert "failedRowsFile" in mock_soda_cloud.find_check_diagnostics(0).keys()


@pytest.mark.parametrize(
    "check, skip_samples",
    [
        pytest.param("- missing_count(cat) = 0", False, id="missing_count"),
        pytest.param("- duplicate_count(cat) = 0", True, id="duplicate_count"),
    ],
)
def test_for_each_checks(check: str, skip_samples: bool, data_source_fixture: DataSourceFixture):
    """Smoke test - just test one basic test to make sure sampler gatekeeper kicks in."""
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()

    scan._configuration.exclude_columns = {table_name: ["cat", "cst_size"]}

    scan.add_sodacl_yaml_str(
        dedent(
            f"""
          for each dataset D:
            datasets:
              - include {table_name}
            checks:
              {check}
        """
        )
    )
    scan.execute()

    scan.assert_no_error_nor_warning_logs()
    if skip_samples:
        scan.assert_log_info("Skipping samples from query")
        assert "failedRowsFile" not in mock_soda_cloud.find_check_diagnostics(0).keys()
    else:
        scan.assert_no_log("Skipping samples from query")
        assert "failedRowsFile" in mock_soda_cloud.find_check_diagnostics(0).keys()


@pytest.mark.parametrize(
    "check",
    [
        pytest.param(
            """- failed rows:
                   fail query: Select * from sodatest_customers_6c2f3574 WHERE cat = 'HIGH' and cst_size < .7""",
            id="failed_rows_expression",
        ),
    ],
)
@pytest.mark.skip(
    reason="Checks with no table associated cannot be checked for excluded sample columns until the FROM part is parsed or something other is implemented"
)
def test_datasource_checks(check: str, data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()

    scan._configuration.exclude_columns = {table_name: ["cat", "cst_size"]}

    scan.add_sodacl_yaml_str(
        dedent(
            f"""
          checks:
            {check}
        """
        )
    )
    scan.execute()

    scan.assert_no_error_nor_warning_logs()
    scan.assert_log_info("Skipping samples from query")
    assert "failedRowsFile" not in mock_soda_cloud.find_check_diagnostics(0).keys()
