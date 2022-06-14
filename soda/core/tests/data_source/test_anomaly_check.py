import pytest
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


@pytest.mark.skip("custom threshold is not supported")
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


@pytest.mark.skip("custom threshold is not supported")
def test_anomaly_detection_fail_with_custom_threshold(scanner: Scanner):
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


@pytest.mark.parametrize(
    "numeric_metric, column",
    [
        pytest.param("duplicate_count", "country", id="duplicate_count"),
        pytest.param("missing_count", "country", id="missing_count"),
        pytest.param("missing_percent", "country", id="missing_percent"),
        pytest.param("min", "size", id="min"),
        pytest.param("avg_length", "country", id="avg_length"),
    ],
)
def test_anomaly_detection_pass_numeric_metrics(numeric_metric, column, scanner):
    import numpy as np

    np.random.seed(61)
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly score for {numeric_metric}({column}) < default
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-{column}-{numeric_metric}",
        metric_values=[10, 10, 10, 9, 8, 0, 0, 0, 0],
    )

    scan.execute()
    scan.assert_all_checks_pass()


def test_anomaly_detection_missing_values(scanner):
    import numpy as np

    np.random.seed(61)
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
              - anomaly score for missing_count(id) < default:
                    missing values: ["ID2"]
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-id-missing_count-1beec996",
        metric_values=[10, 10, 10, 9, 8, 0, 0, 0, 0],
    )

    scan.execute()
    scan.assert_all_checks_pass()


def test_anomaly_detection_invalid_values(scanner):
    import numpy as np

    np.random.seed(61)
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
              - anomaly score for invalid_count(id) < default:
                    valid format: email
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-id-invalid_count-61813b33",
        metric_values=[10, 10, 10, 9, 8, 0, 0, 0, 0],
    )

    scan.execute()
    scan.assert_all_checks_pass()
