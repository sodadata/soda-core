from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_anomaly_detection_default(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly score for row_count < default
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0],
    )

    scan.execute()

    scan.assert_all_checks_pass()


def test_anomaly_detection_custom_threshold(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly score for row_count < .7
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0],
    )

    scan.execute()

    scan.assert_all_checks_warn()


def test_anomaly_detection_fail(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly score for row_count:
                fail: when > .5
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0],
    )

    scan.execute()

    scan.assert_all_checks_fail()
