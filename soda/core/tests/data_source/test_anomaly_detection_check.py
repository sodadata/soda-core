import os

import pytest
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.mock_soda_cloud import TimeGenerator
from soda.cloud.historic_descriptor import (
    HistoricCheckResultsDescriptor,
    HistoricMeasurementsDescriptor,
)


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_historic_descriptors(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    import numpy as np
    from soda.execution.check.anomaly_detection_metric_check import (
        AnomalyDetectionMetricCheck,
    )

    np.random.seed(61)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count
        """
    )
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0],
        time_generator=TimeGenerator(),
    )
    scan.execute(allow_warnings_only=True)
    anomaly_metric_check = scan._checks[0]
    assert isinstance(anomaly_metric_check, AnomalyDetectionMetricCheck)

    historic_measurement_descriptor = anomaly_metric_check.historic_descriptors["historic_measurements"]
    assert historic_measurement_descriptor == HistoricMeasurementsDescriptor(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        limit=1000,
    )

    historic_check_result_descriptor = anomaly_metric_check.historic_descriptors["historic_check_results"]

    # We don't mock check idendity for check results, so we can't compare it
    assert isinstance(historic_check_result_descriptor, HistoricCheckResultsDescriptor)
    assert historic_check_result_descriptor.check_identity is not None
    assert historic_check_result_descriptor.limit == 1000


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_default(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    import numpy as np

    np.random.seed(61)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0],
        time_generator=TimeGenerator(),
    )

    scan.execute(allow_warnings_only=True)

    scan.assert_all_checks_pass()


def test_anomaly_detection_not_enough_data(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=[10],
        time_generator=TimeGenerator(),
    )

    scan.execute(allow_warnings_only=True)
    scan_cloud_result = mock_soda_cloud.pop_scan_result()
    assert scan_cloud_result["checks"][0]["outcomeReasons"] == [
        {
            "code": "not_enough_measurements",
            "message": "Anomaly detection needs at least 4 measurements",
            "severity": "error",
        }
    ]


def test_anomaly_detection_have_no_data(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count
        """
    )
    scan.execute(allow_error_warning=True)
    scan_cloud_result = mock_soda_cloud.pop_scan_result()
    assert scan_cloud_result["checks"][0]["outcomeReasons"] == [
        {
            "code": "not_enough_measurements",
            "message": "Anomaly detection needs at least 4 measurements",
            "severity": "error",
        }
    ]


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
        pytest.param("min", "cst_size", id="min"),
        pytest.param("avg_length", "country", id="avg_length"),
    ],
)
def test_anomaly_detection_pass_numeric_metrics_all_fail(
    numeric_metric: str, column: str, data_source_fixture: DataSourceFixture
) -> None:
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for {numeric_metric}({column})
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-{column}-{numeric_metric}",
        metric_values=[10, 10, 10, 9, 8, 0, 0, 0, 0],
        time_generator=TimeGenerator(),
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
        pytest.param("min", "cst_size", id="min"),
        pytest.param("avg_length", "country", id="avg_length"),
    ],
)
def test_anomaly_detection_pass_numeric_metrics_all_pass(
    numeric_metric: str, column: str, data_source_fixture: DataSourceFixture
) -> None:
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for {numeric_metric}({column})
        """
    )
    # Provide hard to predict training dataset to obtain large intervals to pass the test
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-{column}-{numeric_metric}",
        metric_values=[-100, 100, 200, -300, 500],
        time_generator=TimeGenerator(),
    )

    scan.execute()
    scan.assert_all_checks_pass()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_missing_values_fail(data_source_fixture: DataSourceFixture) -> None:
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
              - anomaly detection for missing_count(id):
                    missing values: ["ID2"]
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-id-missing_count-1beec996",
        metric_values=[10, 10, 10, 9, 8, 0, 0, 0, 0],
        time_generator=TimeGenerator(),
    )

    scan.execute()
    scan.assert_all_checks_fail()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_missing_values_pass(data_source_fixture: DataSourceFixture) -> None:
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
              - anomaly detection for missing_count(id):
                    missing values: ["ID2"]
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-id-missing_count-1beec996",
        metric_values=[3, 4, 5, 6, 7, 8],
        time_generator=TimeGenerator(),
    )

    scan.execute()
    scan.assert_all_checks_pass()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_invalid_values_pass(data_source_fixture: DataSourceFixture) -> None:
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
              - anomaly detection for invalid_count(id):
                    valid format: uuid
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-id-invalid_count-05d677bc",
        metric_values=[10, 10, 10, 9, 8, 0, 0, 0, 0],
        time_generator=TimeGenerator(),
    )

    scan.execute()
    scan.assert_all_checks_pass()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_invalid_values_fail(data_source_fixture: DataSourceFixture) -> None:
    import numpy as np

    np.random.seed(61)
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
              - anomaly detection for invalid_count(id):
                    valid format: uuid
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-id-invalid_count-05d677bc",
        metric_values=[20, 20, 20, 20, 20, 20],
        time_generator=TimeGenerator(),
    )

    scan.execute()
    scan.assert_all_checks_fail()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_incorrect_metric(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
              - anomaly detection for incorrect_metric
        """
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-id-invalid_count-05d677bc",
        metric_values=[10, 10, 10, 9, 8, 0, 0, 0, 0],
        time_generator=TimeGenerator(),
    )

    with pytest.raises(Exception) as e:
        scan.execute()

    assert (
        "An error occurred during the initialization of AnomalyMetricCheck. Please make sure that the metric 'incorrect_metric' is supported. For more information see the docs: https://docs.soda.io/soda-cl/anomaly-detection."
        in str(e.value)
    )


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_training_dataset_parameters(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                name: "Anomaly detection for row_count"
                training_dataset_parameters:
                    frequency: W
                    window_length: 10
                    aggregation_function: first
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0] * 10
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    scan.execute()
    scan.assert_all_checks_pass()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_training_dataset_parameters_incorrect_freq(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                name: "Anomaly detection for row_count"
                training_dataset_parameters:
                    frequency: incorrect_freq
                    window_length: 10
                    aggregation_function: first
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0]
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    with pytest.raises(Exception) as e:
        scan.execute()

    assert "Anomaly Detection: Frequency parameter 'incorrect_freq' is not supported." in str(e.value)


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_training_dataset_parameters_incorrect_aggregation_func(
    data_source_fixture: DataSourceFixture,
) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                name: "Anomaly detection for row_count"
                training_dataset_parameters:
                    frequency: D
                    window_length: 10
                    aggregation_function: invalid_aggregation_func
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0]
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    with pytest.raises(Exception) as e:
        scan.execute()

    assert "Anomaly Detection: Aggregation function 'invalid_aggregation_func' is not supported" in str(e.value)


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_static_hyperparameters(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                model:
                    type: prophet
                    hyperparameters:
                        static:
                            profile:
                                custom_hyperparameters:
                                    changepoint_prior_scale: 0.01
                                    seasonality_prior_scale: 5
                                    seasonality_mode: additive
                                    interval_width: 0.999
                                    changepoint_range: 0.8
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0]
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    scan.execute()
    scan.assert_all_checks_pass()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_static_hyperparameters_built_in_holidays(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                model:
                    type: prophet
                    holidays_country_code: TR
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0]
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    scan.execute()
    scan.assert_all_checks_pass()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_static_hyperparameters_wrong_built_in_holidays(
    data_source_fixture: DataSourceFixture,
) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                model:
                    type: prophet
                    holidays_country_code: invalid_country_code
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0]
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    scan.execute(allow_error_warning=True)
    scan.assert_all_checks_skipped()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_dynamic_hyperparameters_multi_objective(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                name: "Anomaly detection for row_count"
                model:
                    type: prophet
                    hyperparameters:
                        dynamic:
                            objective_metric: ["mape", "rmse"]
                            parallelize_cross_validation: True
                            cross_validation_folds: 2
                            parameter_grid:
                                changepoint_prior_scale: [0.001]
                                seasonality_prior_scale: [0.01, 0.1]
                                seasonality_mode: ["additive"]
                                changepoint_range: [0.8]
                                interval_width: [0.999]
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0]
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    scan.execute()
    scan.assert_all_checks_pass()


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_anomaly_detection_dynamic_hyperparameters_single_objective(data_source_fixture: DataSourceFixture) -> None:
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - anomaly detection for row_count:
                name: "Anomaly detection for row_count"
                model:
                    type: prophet
                    hyperparameters:
                        dynamic:
                            objective_metric: "smape"
                            parallelize_cross_validation: True
                            cross_validation_folds: 2
                            parameter_grid:
                                changepoint_prior_scale: [0.001]
                                seasonality_prior_scale: [0.01, 0.1]
                                seasonality_mode: ["additive"]
                                changepoint_range: [0.8]
                                interval_width: [0.999]
        """
    )
    metric_values = [10, 10, 10, 9, 8, 0, 0, 0, 0]
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count",
        metric_values=metric_values,
        time_generator=TimeGenerator(),
    )
    scan.execute()
    scan.assert_all_checks_pass()
