import logging

import numpy as np
import pandas as pd
from soda.common.logs import Logs
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    ModelConfigs,
    ProphetDefaultHyperparameters,
    TrainingDatasetParameters,
)

from soda.scientific.anomaly_detection_v2.anomaly_detector import AnomalyDetector
from soda.scientific.anomaly_detection_v2.frequency_detector import FrequencyDetector
from soda.scientific.anomaly_detection_v2.models.prophet_model import ProphetDetector


def generate_random_dataframe(size: int, n_rows_to_convert_none: int, frequency: str = "D") -> pd.DataFrame:
    """
    Generate a random dataframe with two columns: 'ds' and 'y'.
    'ds' is an incrementing date starting from today, and 'y' is a random value between 10 and 20.
    The dataframe is of size 'size', with 'rows_to_delete' number of rows randomly removed.

    :param size: The initial size of the dataframe before deleting rows.
    :param rows_to_delete: The number of rows to randomly delete from the dataframe.
    :return: A Pandas dataframe with the specified characteristics.
    """

    # Ensure that the number of rows to delete is not greater than the size of the dataframe
    if n_rows_to_convert_none > size:
        raise ValueError("Number of rows to delete cannot be greater than the dataframe size.")

    # Create a date range starting from 2024-01-01
    dates = pd.date_range(start="2024-01-01", periods=size, freq=frequency)

    # Generate random values between 10 and 20 for 'y'
    # set seed for reproducibility
    np.random.seed(0)
    y_values = np.random.uniform(10, 20, size)

    # Create the dataframe
    df = pd.DataFrame({"ds": dates, "y": y_values})

    # Randomly set consecutive 'y' values to NaN
    df.loc[1:n_rows_to_convert_none, "y"] = None
    df = df.reset_index(drop=True)
    return df


LOGS = Logs(logging.getLogger(__name__))
PARAMS = AnomalyDetector(
    measurements={"results": []},
    check_results={"results": []},
    logs=LOGS,
    model_cfg=ModelConfigs(),
    training_dataset_params=TrainingDatasetParameters(),
)._parse_params()
DAILY_TIME_SERIES_DF = generate_random_dataframe(size=5, n_rows_to_convert_none=0, frequency="D")
HOURLY_TIME_SERIES_DF = generate_random_dataframe(size=5, n_rows_to_convert_none=0, frequency="H")
DAILY_AND_HOURLY_TIME_SERIES_DF = pd.concat([DAILY_TIME_SERIES_DF, HOURLY_TIME_SERIES_DF])
FREQ_DETECTION_RESULT = FrequencyDetector(
    logs=LOGS,
    params=PARAMS,
    time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
    manual_freq="auto",
).detect_frequency()
PROPHET_DETECTOR = ProphetDetector(
    logs=LOGS,
    params=PARAMS,
    time_series_df=DAILY_AND_HOURLY_TIME_SERIES_DF,
    model_cfg=ModelConfigs(),
    training_dataset_params=TrainingDatasetParameters(),
)


def get_alert_level_df(time_series_df: pd.DataFrame) -> pd.DataFrame:
    predictions_df = PROPHET_DETECTOR.setup_fit_predict(
        time_series_df=time_series_df,
        model_hyperparameters=ProphetDefaultHyperparameters(),
    )
    anomalies_df = PROPHET_DETECTOR.detect_anomalies(
        time_series_df=time_series_df,
        predictions_df=predictions_df,
    )
    severity_zones_df = PROPHET_DETECTOR.generate_severity_zones(
        anomalies_df=anomalies_df,
    )
    df_alert_level = PROPHET_DETECTOR.compute_alert_level(
        anomalies_df=severity_zones_df,
    )
    return df_alert_level
