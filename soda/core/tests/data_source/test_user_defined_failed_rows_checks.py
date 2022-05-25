from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_failed_rows_table_expression(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: High customers must have size less than 3
                fail condition: cat = 'HIGH' and size < .7
        """
    )
    scan.execute()

    scan.assert_check_fail()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 1


def test_failed_rows_data_source_query(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - failed rows:
                name: Customers must have size
                fail query: |
                  SELECT *
                  FROM {table_name}
                  WHERE size < 0
        """
    )
    scan.execute()

    scan.assert_check_fail()

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 3


def test_failed_rows_table_query(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - failed rows:
                name: Customers must have size
                fail query: |
                  SELECT *
                  FROM {table_name}
                  WHERE size < 0
        """
    )
    scan.execute()

    scan.assert_check_fail()


def test_bad_failed_rows_query(scanner: Scanner):
    scan = scanner.create_test_scan()
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
