from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd
import pytest
from anomaly_detection_v2.utils import (
    DAILY_AND_HOURLY_TIME_SERIES_DF,
    DAILY_TIME_SERIES_DF,
    FREQ_DETECTION_RESULT,
    LOGS,
    PARAMS,
    PROPHET_DETECTOR,
    generate_random_dataframe,
    get_alert_level_df,
)
from assets.anomaly_detection_assets import (
    df_prophet_model_setup_fit_predict,
    df_prophet_model_setup_fit_predict_holidays,
    test_feedback_processor_seasonality_skip_measurements,
)

from soda.scientific.anomaly_detection_v2.exceptions import (
    AggregationValueError,
    FreqDetectionResultError,
    NotSupportedHolidayCountryError,
)
from soda.scientific.anomaly_detection_v2.feedback_processor import FeedbackProcessor
from soda.scientific.anomaly_detection_v2.models.prophet_model import ProphetDetector
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    HyperparameterConfigs,
    ModelConfigs,
    ProphetDefaultHyperparameters,
    ProphetDynamicHyperparameters,
    ProphetHyperparameterProfiles,
    ProphetParameterGrid,
    TrainingDatasetParameters,
)

from soda.scientific.anomaly_detection_v2.exceptions import (
    AggregationValueError,
    FreqDetectionResultError,
)
from soda.scientific.anomaly_detection_v2.feedback_processor import FeedbackProcessor
from soda.scientific.anomaly_detection_v2.models.prophet_model import ProphetDetector


def test_with_exit() -> None:
    time_series_df = generate_random_dataframe(size=2, n_rows_to_convert_none=0, frequency="D")
    detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        model_cfg=ModelConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
    )
    df_anomalies, frequency_result = detector.run()
    assert df_anomalies.empty
    assert frequency_result.error_code_int == 100
    assert frequency_result.freq_detection_strategy == "not_enough_measurements"


@pytest.mark.parametrize(
    "check_results",
    [
        pytest.param(
            test_feedback_processor_seasonality_skip_measurements,
        )
    ],
)
def test_with_weekly_seasonality_feedback(check_results: dict) -> None:
    time_series_df = generate_random_dataframe(size=5, n_rows_to_convert_none=0, frequency="D")
    df_historic = pd.DataFrame(check_results)
    df_historic = pd.concat([df_historic, time_series_df]).reset_index(drop=True)
    # Override the ds column to be a date rang
    df_historic["ds"] = pd.date_range("2024-01-01", periods=len(df_historic), freq="D")

    feedback_processor = FeedbackProcessor(params=PARAMS, df_historic=df_historic, logs=LOGS)
    has_exogenous_regressor, df_feedback_processed = feedback_processor.get_processed_feedback_df()
    assert has_exogenous_regressor == True
    assert df_feedback_processed.columns.tolist() == ["y", "ds", "skipMeasurements", "external_regressor"]
    assert df_feedback_processed["external_regressor"].values[0] == -0.8325240016225592

    detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=df_feedback_processed,
        model_cfg=ModelConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
        has_exogenous_regressor=has_exogenous_regressor,
    )

    anomalies_df, freq_detection_result = detector.run()
    assert anomalies_df.level.values[0] == "warn"
    assert np.round(anomalies_df.yhat.values[0], 3) == -0.692
    assert np.round(anomalies_df.real_data.values[0], 3) == 14.237
    assert np.round(anomalies_df.critical_greater_than_or_equal.values[0], 3) == 14.241
    assert np.round(anomalies_df.critical_lower_than_or_equal.values[0], 3) == -15.101
    assert np.round(anomalies_df.warning_greater_than_or_equal.values[0], 3) == 11.796
    assert np.round(anomalies_df.warning_lower_than_or_equal.values[0], 3) == -12.655
    assert freq_detection_result.inferred_frequency == "D"
    assert freq_detection_result.freq_detection_strategy == "native_freq"


def test_apply_training_dataset_configs() -> None:
    preprocessed_time_series_df = PROPHET_DETECTOR.apply_training_dataset_configs(
        time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
        freq_detection_result=FREQ_DETECTION_RESULT,
    )
    expected_df = pd.DataFrame(
        [
            {"ds": pd.Timestamp("2024-01-01 00:00:00"), "y": 14.236547993389047},
            {"ds": pd.Timestamp("2024-01-02 00:00:00"), "y": 17.151893663724195},
            {"ds": pd.Timestamp("2024-01-03 00:00:00"), "y": 16.02763376071644},
            {"ds": pd.Timestamp("2024-01-04 00:00:00"), "y": 15.448831829968968},
            {"ds": pd.Timestamp("2024-01-05 00:00:00"), "y": 14.236547993389047},
        ]
    )
    pd.testing.assert_frame_equal(preprocessed_time_series_df, expected_df, check_dtype=False)


def test_apply_training_dataset_configs_with_aggregation_error() -> None:
    with pytest.raises(AggregationValueError):
        training_dataset_configs = TrainingDatasetParameters()
        training_dataset_configs.aggregation_function = "invalid aggregation function"
        prophet_detector = ProphetDetector(
            logs=LOGS,
            params=PARAMS,
            time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
            model_cfg=ModelConfigs(),
            training_dataset_params=training_dataset_configs,
        )
        prophet_detector.apply_training_dataset_configs(
            time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
            freq_detection_result=FREQ_DETECTION_RESULT,
        )


def test_apply_training_dataset_configs_with_frequency_error() -> None:
    with pytest.raises(FreqDetectionResultError):
        modified_freq_detection_result = FREQ_DETECTION_RESULT.model_copy()
        modified_freq_detection_result.inferred_frequency = "invalid frequency"
        prophet_detector = ProphetDetector(
            logs=LOGS,
            params=PARAMS,
            time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
            model_cfg=ModelConfigs(),
            training_dataset_params=TrainingDatasetParameters(),
        )
        prophet_detector.apply_training_dataset_configs(
            time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
            freq_detection_result=modified_freq_detection_result,
        )


def test_not_enough_data() -> None:
    time_series_df = generate_random_dataframe(size=10, n_rows_to_convert_none=7, frequency="D")
    prophet_detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        model_cfg=ModelConfigs(),
        training_dataset_params=TrainingDatasetParameters(),
    )
    df_anomalies, frequency_result = prophet_detector.run()
    assert df_anomalies.empty
    assert frequency_result.error_code_int == 100
    assert frequency_result.freq_detection_strategy == "not_enough_measurements"


def test_get_prophet_hyperparameters_no_tuning() -> None:
    best_hyperparameters = PROPHET_DETECTOR.get_prophet_hyperparameters(
        time_series_df=DAILY_TIME_SERIES_DF,
    )
    dafault_hyperparameters = ProphetDefaultHyperparameters()
    assert best_hyperparameters == dafault_hyperparameters


def test_get_prophet_hyperparameters_invalid_obhective_metric() -> None:
    with pytest.raises(ValueError):
        HyperparameterConfigs(
            static=ProphetHyperparameterProfiles(), dynamic=ProphetDynamicHyperparameters(objective_metric="invalid")
        )


def test_get_prophet_hyperparameters_with_not_enough_data() -> None:
    model_cfg = ModelConfigs(
        hyperparameters=HyperparameterConfigs(
            static=ProphetHyperparameterProfiles(), dynamic=ProphetDynamicHyperparameters(objective_metric="smape")
        )
    )
    prophet_detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=DAILY_TIME_SERIES_DF,
        model_cfg=model_cfg,
        training_dataset_params=TrainingDatasetParameters(),
    )
    best_hyperparameters = prophet_detector.get_prophet_hyperparameters(
        time_series_df=DAILY_TIME_SERIES_DF,
    )
    dafault_hyperparameters = ProphetDefaultHyperparameters()
    assert best_hyperparameters == dafault_hyperparameters


@pytest.mark.parametrize(
    "objective_metric, expected_changepoint_prior_scale, expected_seasonality_prior_scale",
    [
        pytest.param(
            "smape",
            0.1,
            0.05,
            id="smape objective metric",
        ),
        pytest.param(
            ["coverage", "mdape"],
            0.05,
            0.1,
            id="coverage and smape multi objective metric",
        ),
    ],
)
def test_get_prophet_hyperparameters_with_tuning(
    objective_metric: Union[str, list[str]],
    expected_changepoint_prior_scale: float,
    expected_seasonality_prior_scale: float,
) -> None:
    time_series_df = generate_random_dataframe(size=20, n_rows_to_convert_none=0, frequency="D")
    model_cfg = ModelConfigs(
        hyperparameters=HyperparameterConfigs(
            static=ProphetHyperparameterProfiles(),
            dynamic=ProphetDynamicHyperparameters(
                objective_metric=objective_metric,
                parallelize_cross_validation=False,
                parameter_grid=ProphetParameterGrid(
                    changepoint_prior_scale=[0.05, 0.1],
                    seasonality_prior_scale=[0.05, 0.1],
                ),
            ),
        )
    )

    prophet_detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        model_cfg=model_cfg,
        training_dataset_params=TrainingDatasetParameters(),
    )
    best_hyperparameters = prophet_detector.get_prophet_hyperparameters(
        time_series_df=time_series_df,
    )
    dafault_hyperparameters = ProphetDefaultHyperparameters()
    expected_hyperparameters = ProphetDefaultHyperparameters(
        changepoint_prior_scale=expected_changepoint_prior_scale,
        seasonality_prior_scale=expected_seasonality_prior_scale,
    )
    assert best_hyperparameters != dafault_hyperparameters
    assert best_hyperparameters == expected_hyperparameters


def test_setup_fit_predict() -> None:
    predictions_df = PROPHET_DETECTOR.setup_fit_predict(
        time_series_df=DAILY_TIME_SERIES_DF,
        model_hyperparameters=ProphetDefaultHyperparameters(),
    )
    # Test only the columns that are needed for the test
    predictions_df = predictions_df[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    pd.testing.assert_frame_equal(predictions_df, df_prophet_model_setup_fit_predict, check_dtype=False)


def test_setup_fit_predict_holidays() -> None:
    prophet_detector = ProphetDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
        model_cfg=ModelConfigs(holidays_country_code="TR"),
        training_dataset_params=TrainingDatasetParameters(),
    )
    predictions_df = prophet_detector.setup_fit_predict(
        time_series_df=DAILY_TIME_SERIES_DF, model_hyperparameters=ProphetDefaultHyperparameters()
    )
    predictions_df = predictions_df[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    pd.testing.assert_frame_equal(predictions_df, df_prophet_model_setup_fit_predict_holidays, check_dtype=False)


def test_setup_fit_predic_holidays_invalid_country() -> None:
    with pytest.raises(NotSupportedHolidayCountryError):
        prophet_detector = ProphetDetector(
            logs=LOGS,
            params=PARAMS,
            time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
            model_cfg=ModelConfigs(holidays_country_code="invalid_country_code"),
            training_dataset_params=TrainingDatasetParameters(),
        )
        prophet_detector.setup_fit_predict(
            time_series_df=DAILY_TIME_SERIES_DF,
            model_hyperparameters=ProphetDefaultHyperparameters(),
        )


def test_detect_anomalies() -> None:
    predictions_df = PROPHET_DETECTOR.setup_fit_predict(
        time_series_df=DAILY_TIME_SERIES_DF,
        model_hyperparameters=ProphetDefaultHyperparameters(),
    )
    anomalies_df = PROPHET_DETECTOR.detect_anomalies(
        time_series_df=DAILY_TIME_SERIES_DF,
        predictions_df=predictions_df,
    )
    anomalies_df = anomalies_df[["ds", "yhat", "yhat_lower", "yhat_upper", "real_data", "is_anomaly"]]
    expected_df = pd.DataFrame(
        [
            {
                "ds": pd.Timestamp("2024-01-05 00:00:00"),
                "yhat": 15.718782053683304,
                "yhat_lower": 13.7886123341,
                "yhat_upper": 17.7467321317,
                "real_data": 14.2365479934,
                "is_anomaly": 0,
            }
        ]
    )
    pd.testing.assert_frame_equal(anomalies_df, expected_df, check_dtype=False)


def test_detect_anomalies_with_integer() -> None:
    integer_time_series_df = DAILY_TIME_SERIES_DF.copy()
    integer_time_series_df["y"] = integer_time_series_df["y"].astype(int)
    predictions_df = PROPHET_DETECTOR.setup_fit_predict(
        time_series_df=integer_time_series_df,
        model_hyperparameters=ProphetDefaultHyperparameters(),
    )
    anomalies_df = PROPHET_DETECTOR.detect_anomalies(
        time_series_df=integer_time_series_df,
        predictions_df=predictions_df,
    )
    anomalies_df = anomalies_df[["ds", "yhat", "yhat_lower", "yhat_upper", "real_data", "is_anomaly"]]
    expected_df = pd.DataFrame(
        [
            {
                "ds": pd.Timestamp("2024-01-05 00:00:00"),
                "yhat": 15.561378400750227,
                "yhat_lower": 13.0,
                "yhat_upper": 18.0,
                "real_data": 14,
                "is_anomaly": 0,
            }
        ]
    )
    pd.testing.assert_frame_equal(anomalies_df, expected_df, check_dtype=False)


def test_detect_anomalies_with_tight_bounds() -> None:
    time_series_df = generate_random_dataframe(size=30, n_rows_to_convert_none=0, frequency="D")
    time_series_df["y"] = 61.61  # Set all values to 61 to create tight bounds
    predictions_df = PROPHET_DETECTOR.setup_fit_predict(
        time_series_df=time_series_df,
        model_hyperparameters=ProphetDefaultHyperparameters(),
    )
    anomalies_df = PROPHET_DETECTOR.detect_anomalies(
        time_series_df=time_series_df,
        predictions_df=predictions_df,
    )
    anomalies_df = anomalies_df[["ds", "yhat", "yhat_lower", "yhat_upper", "real_data", "is_anomaly"]]
    expected_df = pd.DataFrame(
        [
            {
                "ds": pd.Timestamp("2024-01-30 00:00:00"),
                "yhat": 61.61,
                "yhat_lower": 61.60989978229838,
                "yhat_upper": 61.610100181230486,
                "real_data": 61.61,
                "is_anomaly": 0,
            }
        ]
    )
    pd.testing.assert_frame_equal(anomalies_df, expected_df, check_dtype=False)


def test_detect_anomalies_with_anomaly() -> None:
    time_series_df = generate_random_dataframe(size=10, n_rows_to_convert_none=0, frequency="D")
    # Set last value to 10000 to create an anomaly
    time_series_df.loc[time_series_df.index[-1], "y"] = 10000
    predictions_df = PROPHET_DETECTOR.setup_fit_predict(
        time_series_df=time_series_df,
        model_hyperparameters=ProphetDefaultHyperparameters(),
    )
    anomalies_df = PROPHET_DETECTOR.detect_anomalies(
        time_series_df=time_series_df,
        predictions_df=predictions_df,
    )
    anomalies_df = anomalies_df[["ds", "yhat", "yhat_lower", "yhat_upper", "real_data", "is_anomaly"]]
    expected_df = pd.DataFrame(
        [
            {
                "ds": pd.Timestamp("2024-01-10 00:00:00"),
                "yhat": 17.983157287127312,
                "yhat_lower": 13.7803360158,
                "yhat_upper": 22.0704573273,
                "real_data": 10000.0,
                "is_anomaly": 1,
            }
        ]
    )
    pd.testing.assert_frame_equal(anomalies_df, expected_df, check_dtype=False)


def test_generate_severity_zones() -> None:
    predictions_df = PROPHET_DETECTOR.setup_fit_predict(
        time_series_df=DAILY_TIME_SERIES_DF,
        model_hyperparameters=ProphetDefaultHyperparameters(),
    )
    anomalies_df = PROPHET_DETECTOR.detect_anomalies(
        time_series_df=DAILY_TIME_SERIES_DF,
        predictions_df=predictions_df,
    )
    severity_zones_df = PROPHET_DETECTOR.generate_severity_zones(
        anomalies_df=anomalies_df,
    )
    severity_zones_df = severity_zones_df[
        [
            "is_anomaly",
            "critical_greater_than_or_equal",
            "critical_lower_than_or_equal",
            "warning_greater_than_or_equal",
            "warning_lower_than_or_equal",
        ]
    ]
    expected_df = pd.DataFrame(
        [
            {
                "is_anomaly": 0,
                "critical_greater_than_or_equal": 18.14254411146,
                "critical_lower_than_or_equal": 13.39280035434,
                "warning_greater_than_or_equal": 17.7467321317,
                "warning_lower_than_or_equal": 13.7886123341,
            }
        ]
    )
    pd.testing.assert_frame_equal(severity_zones_df, expected_df, check_dtype=False)


def test_compute_alert_level_pass() -> None:
    df_alert_level = get_alert_level_df(DAILY_TIME_SERIES_DF)
    level = df_alert_level["level"].values[0]
    assert level == "pass"


def test_compute_alert_level_warn() -> None:
    time_series_df = DAILY_TIME_SERIES_DF.copy()
    time_series_df.loc[time_series_df.index[-1], "y"] = 13.6
    df_alert_level = get_alert_level_df(time_series_df)
    level = df_alert_level["level"].values[0]
    assert level == "warn"


def test_compute_alert_level_fail() -> None:
    time_series_df = DAILY_TIME_SERIES_DF.copy()
    time_series_df.loc[time_series_df.index[-1], "y"] = 12
    df_alert_level = get_alert_level_df(time_series_df)
    level = df_alert_level["level"].values[0]
    assert level == "fail"
