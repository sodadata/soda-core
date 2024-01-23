import logging

import pandas as pd
import pytest
from anomaly_detection_v2.utils import PARAMS
from assets.anomaly_detection_assets import (
    test_anomaly_detector_parsed_ad_measurements,
    test_empty_anomaly_detector_parsed_ad_measurements,
    test_feedback_processor_feedback_processed_df,
    test_feedback_processor_seasonality_skip_measurements,
    test_feedback_processor_seasonality_skip_measurements_expectation,
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
    has_exogenous_regressor, df = feedback_processor.get_processed_feedback_df()
    assert has_exogenous_regressor == False
    pd.testing.assert_frame_equal(df, expectation, check_dtype=False)


@pytest.mark.parametrize(
    "check_results, expected_processed_feedback_df",
    [
        pytest.param(
            test_feedback_processor_seasonality_skip_measurements,
            test_feedback_processor_seasonality_skip_measurements_expectation,
        )
    ],
)
def test_feedback_processor_with_exogenous_regressor(
    check_results: dict, expected_processed_feedback_df: pd.DataFrame
) -> None:
    df_historic = pd.DataFrame(check_results)
    df_historic["ds"] = pd.to_datetime(df_historic["ds"])
    expected_processed_feedback = pd.DataFrame(expected_processed_feedback_df)
    expected_processed_feedback["ds"] = pd.to_datetime(expected_processed_feedback["ds"])

    feedback_processor = FeedbackProcessor(params=PARAMS, df_historic=df_historic, logs=LOGS)
    has_exogenous_regressor, df_feedback_processed = feedback_processor.get_processed_feedback_df()
    assert has_exogenous_regressor is True
    pd.testing.assert_frame_equal(df_feedback_processed, expected_processed_feedback, check_dtype=False)
