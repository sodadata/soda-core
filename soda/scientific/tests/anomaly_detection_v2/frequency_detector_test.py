from __future__ import annotations

import logging

import pandas as pd
from anomaly_detection_v2.utils import generate_random_dataframe
from soda.common.logs import Logs
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    ModelConfigs,
    SeverityLevelParameters,
    TrainingDatasetParameters,
)

from soda.scientific.anomaly_detection_v2.anomaly_detector import AnomalyDetector
from soda.scientific.anomaly_detection_v2.frequency_detector import FrequencyDetector

LOGS = Logs(logging.getLogger(__name__))
PARAMS = AnomalyDetector(
    measurements={"results": []},
    check_results={"results": []},
    logs=LOGS,
    model_cfg=ModelConfigs(),
    training_dataset_params=TrainingDatasetParameters(),
    severity_level_params=SeverityLevelParameters(),
)._parse_params()


def test_auto_daily_frequency_detector() -> None:
    time_series_df = generate_random_dataframe(size=10, n_rows_to_convert_none=0, frequency="D")
    frequency_detector = FrequencyDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        manual_freq="auto",
    )
    frequency_detector_result = frequency_detector.detect_frequency()
    assert frequency_detector_result.inferred_frequency == "D"
    assert frequency_detector_result.freq_detection_strategy == "native_freq"
    assert frequency_detector_result.error_code_int == 0
    assert frequency_detector_result.error_severity == "info"
    assert frequency_detector_result.error_message == "Frequency Detection Info: native frequency detected"


def test_auto_hourly_frequency_detector() -> None:
    time_series_df = generate_random_dataframe(size=10, n_rows_to_convert_none=0, frequency="H")
    frequency_detector = FrequencyDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        manual_freq="auto",
    )
    frequency_detector_result = frequency_detector.detect_frequency()
    assert frequency_detector_result.inferred_frequency == "H"
    assert frequency_detector_result.freq_detection_strategy == "native_freq"
    assert frequency_detector_result.error_code_int == 0
    assert frequency_detector_result.error_severity == "info"
    assert frequency_detector_result.error_message == "Frequency Detection Info: native frequency detected"


def test_not_enough_data_frequency_detector() -> None:
    time_series_df = generate_random_dataframe(size=2, n_rows_to_convert_none=0, frequency="D")
    frequency_detector = FrequencyDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        manual_freq="auto",
    )
    frequency_detector_result = frequency_detector.detect_frequency()
    assert frequency_detector_result.inferred_frequency == None
    assert frequency_detector_result.freq_detection_strategy == "not_enough_measurements_custom"
    assert frequency_detector_result.error_code_int == 100
    assert frequency_detector_result.error_severity == "error"
    assert frequency_detector_result.error_message == (
        "Anomaly Detection Insufficient Training Data Warning: "
        "The model requires a minimum of 4 historical measurements"
        " for accurate predictions, but currently has only 2 check"
        " results available."
    )


def test_coerced_daily_frequency_detector() -> None:
    time_series_df = generate_random_dataframe(size=10, n_rows_to_convert_none=0, frequency="D")
    time_series_repeated_df = pd.concat([time_series_df, time_series_df, time_series_df])

    frequency_detector = FrequencyDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_repeated_df,
        manual_freq="auto",
    )
    frequency_detector_result = frequency_detector.detect_frequency()
    assert frequency_detector_result.inferred_frequency == "D"
    assert frequency_detector_result.freq_detection_strategy == "coerced_daily"
    assert frequency_detector_result.error_code_int == 0
    assert frequency_detector_result.error_severity == "warn"
    assert frequency_detector_result.error_message == (
        "Frequency Detection Warning: Due to unpredictable time intervals, "
        "we have assumed a daily frequency. If more than 1 data point occurs "
        "in one day we take the last record. Free free to set the "
        "frequency manually in your SodaCL."
    )


def test_no_duplicate_dates_daily_frequency_detector() -> None:
    time_series_df = generate_random_dataframe(size=10, n_rows_to_convert_none=0, frequency="D")
    # Drop index between 2 and 5
    time_series_df = time_series_df.drop([2, 3, 4])
    frequency_detector = FrequencyDetector(
        logs=LOGS,
        params=PARAMS,
        time_series_df=time_series_df,
        manual_freq="auto",
    )
    frequency_detector_result = frequency_detector.detect_frequency()
    assert frequency_detector_result.inferred_frequency == "D"
    assert frequency_detector_result.freq_detection_strategy == "converted_daily_no_dupes"
    assert frequency_detector_result.error_code_int == 0
    assert frequency_detector_result.error_severity == "info"
    assert frequency_detector_result.error_message == (
        "Frequency Detection Info: Converted to daily frequency no dupes with time info removed"
    )
