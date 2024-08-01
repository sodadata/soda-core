from helpers.common_test_tables import customers_huge_test_table, customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.mock_http_request import MockHttpRequest
from helpers.mock_http_sampler import MockHttpSampler


def test_failed_rows_table_expression_with_limit(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: failed rows with limit
                samples limit: 1
                fail condition: cat = 'HIGH' and cst_size > 0
        """
    )
    scan.execute()
    scan.assert_check_fail()
    assert mock_soda_cloud.find_failed_rows_line_count(0) == 1


def test_failed_rows_table_expression(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: High customers must have cst_size less than 3
                fail condition: cat = 'HIGH' and cst_size < .7
        """
    )
    scan.execute()

    scan.assert_check_fail()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 1


def test_failed_rows_data_source_query_with_limit(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                samples limit: 1
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size > 0
        """
    )
    scan.execute()

    scan.assert_check_fail()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 1


def test_failed_rows_data_source_query(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < 0
        """
    )
    scan.execute()

    scan.assert_check_fail()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 3


def test_failed_rows_http_samples_present(
    data_source_fixture: DataSourceFixture, mock_http_post_request: MockHttpRequest
):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler(MockHttpSampler)
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < 0
        """
    )
    scan.execute()

    scan.assert_check_fail()

    mock_http_post_request.assert_failed_rows_http_samples_present("Customers must have cst_size")


def test_failed_rows_http_samples_absent(
    data_source_fixture: DataSourceFixture, mock_http_post_request: MockHttpRequest
):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler(MockHttpSampler)
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE country IS NULL
        """
    )
    scan.execute()

    scan.assert_check_pass()

    mock_http_post_request.assert_failed_rows_http_samples_absent("Customers must have cst_size")


def test_failed_rows_table_query(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < 0
        """
    )
    scan.execute()

    scan.assert_check_fail()


def test_failed_rows_table_query_with_variables(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    scan.add_variables({"size_count": "0"})
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < ${{size_count}}
        """
    )
    scan.execute()

    scan.assert_check_fail()


def test_failed_rows_table_query_with_limit(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: Customers must have cst_size
                samples limit: 1
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
        """
    )
    scan.execute()

    scan.assert_check_fail()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 1


def test_bad_failed_rows_query(data_source_fixture: DataSourceFixture):
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Bad query
                fail query: |
                  SELECT MAKE THIS BREAK !
        """
    )
    scan.execute_unchecked()

    assert len(scan.get_error_logs()) > 0
    assert len(scan._queries) == 1
    assert scan._queries[0].exception is not None

    scan_result = mock_soda_cloud.pop_scan_result()
    logs = scan_result.get("logs")
    first_error_log = next(log for log in logs if log["level"] == "error")
    assert "location" in first_error_log


def test_failed_rows_query_warn_threshold(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < 0
                warn: when = 3
                fail: when > 5
        """
    )
    scan.execute()
    scan.assert_check_warn()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 3


def test_failed_rows_query_fail_threshold(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < 0
                warn: when = 3
                fail: when > 2
        """
    )
    scan.execute()
    scan.assert_check_fail()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 3


def test_failed_rows_query_fail_threshold_pass(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < 0
                fail: when > 3
        """
    )
    scan.execute()
    scan.assert_check_pass()


def test_failed_rows_query_warn_threshold_pass(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have cst_size
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
                  WHERE cst_size < 0
                warn: when > 3
        """
    )
    scan.execute()
    scan.assert_check_pass()


def test_failed_rows_condition_fail_threshold_pass(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: Customers must have cst_size
                fail condition: cst_size < 0
                fail: when > 3
        """
    )
    scan.execute()
    scan.assert_check_pass()


def test_failed_rows_condition_warn_threshold_pass(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: Customers must have cst_size
                fail condition: cst_size < 0
                warn: when > 3
        """
    )
    scan.execute()
    scan.assert_check_pass()


def test_failed_rows_over_100_failed(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_huge_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must be empty
                fail query: |
                  SELECT *
                  FROM {qualified_table_name}
        """
    )
    scan.execute()
    scan.assert_check_fail()
    check_metric_name = mock_soda_cloud.find_check(0)["metrics"][0]

    assert mock_soda_cloud.find_check_metric(check_metric_name)["value"] == 120
    assert mock_soda_cloud.find_failed_rows_line_count(0) == 100
