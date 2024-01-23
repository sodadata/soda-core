from __future__ import annotations
import logging

import pandas as pd
import pytest
from anomaly_detection_v2.utils import generate_random_dataframe
from assets.anomaly_detection_assets import (
    test_prophet_model_skip_measurements_previousAndThis,
    test_prophet_model_skip_measurements_previousAndThis_expectation,
    test_prophet_model_skip_measurements_previousAndThis_last_measurement,
    test_prophet_model_skip_measurements_this_exclusive_previous,
    test_prophet_model_skip_measurements_this_exclusive_previous_expectation,
)
from soda.common.logs import Logs
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    HyperparameterConfigs,
    ModelConfigs,
    TrainingDatasetParameters,
)

from soda.scientific.anomaly_detection_v2.anomaly_detector import AnomalyDetector
from soda.scientific.anomaly_detection_v2.models.prophet_model import ProphetDetector
from soda.scientific.anomaly_detection_v2.pydantic_models import FreqDetectionResult

LOGS = Logs(logging.getLogger(__name__))
PARAMS = AnomalyDetector(
    measurements={"results": []},
    check_results={"results": []},
    logs=LOGS,
    model_cfg=ModelConfigs(),
    training_dataset_params=TrainingDatasetParameters(),
)._parse_params()
FREQ_DETECTION_RESULTS = FreqDetectionResult(
    inferred_frequency="D",
    df=pd.DataFrame(),
    freq_detection_strategy="",
    error_code_int=0,
    error_code="",
    error_severity="",
    error_message="",
)


@pytest.mark.parametrize(
    "time_series_df, expected_time_series_df",
    [
        pytest.param(
            test_prophet_model_skip_measurements_this_exclusive_previous,
            test_prophet_model_skip_measurements_this_exclusive_previous_expectation,
            id="this and exclusive previous",
        ),
        pytest.param(
            test_prophet_model_skip_measurements_previousAndThis,
            test_prophet_model_skip_measurements_previousAndThis_expectation,
            id="previousAndThis",
        ),
        pytest.param(
            test_prophet_model_skip_measurements_previousAndThis_last_measurement,
            pd.DataFrame(columns=["ds", "y"], index=pd.RangeIndex(start=0, stop=0, step=1)),
            id="previousAndThis on last measurement",
        ),
    ],
)
def test_base_model_preprocess(time_series_df: pd.DataFrame, expected_time_series_df: pd.DataFrame) -> None:
    detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        hyperparamaters_cfg=HyperparameterConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
    )
    df_preprocessed = detector.preprocess(time_series_df=time_series_df)
    pd.testing.assert_frame_equal(df_preprocessed, expected_time_series_df, check_dtype=False)


@pytest.mark.parametrize(
    "size, n_rows_to_convert_none, expected_n_rows",
    [
        pytest.param(
            100,
            10,
            89,
            id="DF having 100 rows and 10 rows are skipped",
        ),
        pytest.param(
            100,
            8,
            100,
            id="DF having 100 rows and 8 rows are skipped",
        ),
        pytest.param(
            10,
            3,
            6,
            id="DF having 10 rows and 3 rows are skipped",
        ),
        pytest.param(
            10,
            2,
            10,
            id="DF having 10 rows and 2 rows are skipped",
        ),
        pytest.param(
            30,
            4,
            30,
            id="DF having 30 rows and 4 rows are skipped",
        ),
        pytest.param(
            30,
            6,
            23,
            id="DF having 30 rows and 6 rows are skipped",
        ),
    ],
)
def test_base_model_remove_big_gaps(size: int, n_rows_to_convert_none: int, expected_n_rows: int) -> None:
    time_series_df = generate_random_dataframe(size, n_rows_to_convert_none)
    detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        hyperparamaters_cfg=HyperparameterConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
    )
    df_preprocessed = detector.remove_big_gaps_from_time_series(
        time_series_df=time_series_df, freq_detection_result=FREQ_DETECTION_RESULTS
    )
    assert df_preprocessed.shape[0] == expected_n_rows
