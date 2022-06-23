import os

import pytest
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.data_source_fixture import DataSourceFixture


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_default(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    import numpy as np

    np.random.seed(61)

    scan = data_source_fixture.create_test_scan()

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

    scan.execute(allow_warnings_only=True)

    scan.assert_all_checks_pass()


def test_anomaly_detection_not_enough_data(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly score for row_count < default
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=[10],
    )

    scan.execute(allow_warnings_only=True)
    scan_cloud_result = mock_soda_cloud.pop_scan_result()
    assert scan_cloud_result["checks"][0]["outcomeReasons"] == [
        {
            "code": "not_enough_measurements",
            "message": "Anomaly detection needs at least 5 measurements",
            "severity": "error",
        }
    ]


def test_anomaly_detection_have_no_data(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly score for row_count < default
        """
    )
    scan.execute(allow_error_warning=True)
    scan_cloud_result = mock_soda_cloud.pop_scan_result()
    assert scan_cloud_result["checks"][0]["outcomeReasons"] == [
        {
            "code": "not_enough_measurements",
            "message": "Anomaly detection needs at least 5 measurements",
            "severity": "error",
        }
    ]


@pytest.mark.skip("custom threshold is not supported")
def test_anomaly_detection_custom_threshold(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

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
def test_anomaly_detection_fail_with_custom_threshold(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

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


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
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
def test_anomaly_detection_pass_numeric_metrics(numeric_metric, column, data_source_fixture):
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

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


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_missing_values(data_source_fixture):
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

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


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_invalid_values(data_source_fixture):
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

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
