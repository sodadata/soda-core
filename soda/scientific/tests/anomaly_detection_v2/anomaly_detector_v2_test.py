import logging

import numpy as np
import pandas as pd
import pytest
from assets.anomaly_detection_assets import (
    test_anomaly_detector_evaluate_historic_check_results,
    test_anomaly_detector_evaluate_historic_measurements,
    test_anomaly_detector_parsed_ad_measurements,
    test_anomaly_detector_parsed_empty_historic_check_results,
    test_anomaly_detector_parsed_historic_check_results,
    test_anomaly_detector_parsed_historical_measurements,
    test_empty_anomaly_detector_parsed_ad_measurements,
)
from soda.common.logs import Logs
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    ModelConfigs,
    TrainingDatasetParameters,
)

from soda.scientific.anomaly_detection_v2.anomaly_detector import AnomalyDetector

LOGS = Logs(logging.getLogger(__name__))


@pytest.mark.parametrize(
    "measurements, expectation",
    [
        pytest.param(
            test_anomaly_detector_evaluate_historic_measurements,
            test_anomaly_detector_parsed_historical_measurements,
            id="Test historical measurement parsing",
        )
    ],
)
def test_historical_measurements_parsing(measurements: dict, expectation: pd.DataFrame) -> None:
    detector = AnomalyDetector(
        measurements=measurements,
        check_results={},
        model_cfg=ModelConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
        logs=LOGS,
    )
    df_historical_measurements = detector._parse_historical_measurements()
    pd.testing.assert_frame_equal(df_historical_measurements, expectation, check_dtype=False)


def test_empty_historic_measurements_parsing() -> None:
    with pytest.raises(ValueError):
        detector = AnomalyDetector(
            measurements={},
            check_results={},
            model_cfg=ModelConfigs(),
            training_dataset_params=TrainingDatasetParameters(),
            logs=LOGS,
        )
        detector._parse_historical_measurements()


@pytest.mark.parametrize(
    "check_results, expectation",
    [
        pytest.param(
            test_anomaly_detector_evaluate_historic_check_results,
            test_anomaly_detector_parsed_historic_check_results,
            id="Test historical check results parsing",
        ),
        pytest.param(
            {},
            test_anomaly_detector_parsed_empty_historic_check_results,
            id="Test empty historical check results parsing",
        ),
    ],
)
def test_historical_check_results_parsing(check_results: dict, expectation: pd.DataFrame) -> None:
    detector = AnomalyDetector(
        measurements={},
        check_results=check_results,
        model_cfg=ModelConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
        logs=LOGS,
    )
    df_check_results = detector._parse_historical_check_results()
    pd.testing.assert_frame_equal(df_check_results, expectation, check_dtype=False)


@pytest.mark.parametrize(
    "measurements, check_results, expectation",
    [
        pytest.param(
            test_anomaly_detector_evaluate_historic_measurements,
            test_anomaly_detector_evaluate_historic_check_results,
            test_anomaly_detector_parsed_ad_measurements,
            id="Test historical anomaly detection df",
        ),
        pytest.param(
            test_anomaly_detector_evaluate_historic_measurements,
            {},
            test_empty_anomaly_detector_parsed_ad_measurements,
            id="Test historical anomaly detection df with empty check results",
        ),
    ],
)
def test_historical_anomaly_detection_df(measurements: dict, check_results: dict, expectation: pd.DataFrame) -> None:
    detector = AnomalyDetector(
        measurements=measurements,
        check_results=check_results,
        model_cfg=ModelConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
        logs=LOGS,
    )
    df_historical = detector._generate_historical_ad_df()
    pd.testing.assert_frame_equal(df_historical, expectation, check_dtype=False)


@pytest.mark.parametrize(
    "measurements, check_results",
    [
        pytest.param(
            test_anomaly_detector_evaluate_historic_measurements,
            test_anomaly_detector_evaluate_historic_check_results,
        )
    ],
)
def test_anomaly_detector_evaluate(measurements: dict, check_results: dict) -> None:
    detector = AnomalyDetector(
        measurements=measurements,
        check_results=check_results,
        model_cfg=ModelConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
        logs=LOGS,
    )
    level, diagnostic = detector.evaluate()
    anomaly_predicted_value = np.round(diagnostic["anomalyPredictedValue"], 3)
    assert level == "pass"
    assert anomaly_predicted_value == 9.645
