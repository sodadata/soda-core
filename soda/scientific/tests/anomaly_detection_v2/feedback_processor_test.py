from __future__ import annotations

import copy
import logging

import pandas as pd
import pytest
from anomaly_detection_v2.utils import PARAMS
from assets.anomaly_detection_assets import (
    test_anomaly_detector_parsed_ad_measurements,
    test_empty_anomaly_detector_parsed_ad_measurements,
    test_feedback_processor_correctly_classified_anomalies,
    test_feedback_processor_correctly_classified_anomalies_expectation,
    test_feedback_processor_feedback_processed_df,
    test_feedback_processor_monthly_seasonality_expectation,
    test_feedback_processor_seasonality_skip_measurements,
    test_feedback_processor_weekly_seasonality_expectation,
    test_feedback_processor_yearly_seasonality_expectation,
)
from soda.common.logs import Logs

from soda.scientific.anomaly_detection_v2.feedback_processor import FeedbackProcessor

LOGS = Logs(logging.getLogger(__name__))


@pytest.mark.parametrize(
    "params, df_historic, expectation",
    [
        pytest.param(
            {},
            test_anomaly_detector_parsed_ad_measurements,
            test_feedback_processor_feedback_processed_df,
            id="Test feedback processor feedback processed df",
        ),
        pytest.param(
            {},
            test_empty_anomaly_detector_parsed_ad_measurements,
            test_empty_anomaly_detector_parsed_ad_measurements,
            id="Test feedback processor feedback processed df with empty feedbacks",
        ),
        pytest.param(
            {},
            test_feedback_processor_correctly_classified_anomalies,
            test_feedback_processor_correctly_classified_anomalies_expectation,
            id="Test feedback processor with correctly classified anomalies",
        ),
    ],
)
def test_feedback_processor_get_processed_feedback_df(
    params: dict, df_historic: pd.DataFrame, expectation: pd.DataFrame
) -> None:
    feedback_processor = FeedbackProcessor(
        params=params,
        df_historic=df_historic,
        logs=LOGS,
    )
    has_external_regressor, df = feedback_processor.get_processed_feedback_df()
    assert has_external_regressor == False
    pd.testing.assert_frame_equal(df, expectation, check_dtype=False)


@pytest.mark.parametrize(
    "reason, expected_regressor_column_name, expected_output",
    [
        pytest.param(
            "expectedWeeklySeasonality",
            "external_regressor_weekly",
            test_feedback_processor_weekly_seasonality_expectation,
            id="Test feedback processor with weekly seasonality external regressor",
        ),
        pytest.param(
            "expectedMonthlySeasonality",
            "external_regressor_monthly",
            test_feedback_processor_monthly_seasonality_expectation,
            id="Test feedback processor with monthly seasonality external regressor",
        ),
        pytest.param(
            "expectedYearlySeasonality",
            "external_regressor_yearly",
            test_feedback_processor_yearly_seasonality_expectation,
            id="Test feedback processor with yearly seasonality external regressor",
        ),
    ],
)
def test_feedback_processor_external_regressors(
    reason: str, expected_regressor_column_name: str, expected_output: dict
) -> None:
    input_data = copy.deepcopy(test_feedback_processor_seasonality_skip_measurements)
    # change the reason to expectedMonthlySeasonality to test monthly seasonality
    input_data["feedback"][0]["reason"] = reason  # type: ignore
    df_historic = pd.DataFrame(input_data)
    df_historic["ds"] = pd.to_datetime(df_historic["ds"])
    feedback_processor = FeedbackProcessor(params=PARAMS, df_historic=df_historic, logs=LOGS)
    has_external_regressor, df_feedback_processed = feedback_processor.get_processed_feedback_df()
    assert has_external_regressor is True
    filtered_columns = ["ds", "y", expected_regressor_column_name]
    df_feedback_processed = df_feedback_processed[
        [col for col in filtered_columns if col in df_feedback_processed.columns]
    ]
    df_expected = pd.DataFrame(expected_output)
    df_expected["ds"] = pd.to_datetime(df_expected["ds"])
    pd.testing.assert_frame_equal(df_feedback_processed, df_expected, check_dtype=False)
