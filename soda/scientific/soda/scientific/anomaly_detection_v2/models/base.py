"""ABC for Detectors."""

from abc import ABC, abstractmethod

import pandas as pd
from soda.common.logs import Logs
from soda.scientific.anomaly_detection_v2.globals import EXTERNAL_REGRESSOR_COLUMNS
from soda.scientific.anomaly_detection_v2.pydantic_models import FreqDetectionResult


class BaseDetector(ABC):
    """BaseDetector."""

    def __init__(self, logs: Logs, time_series_df: pd.DataFrame) -> None:
        self.logs = logs
        self.time_series_df = time_series_df

    @abstractmethod
    def run(self) -> pd.DataFrame:
        raise NotImplementedError("You must implement a `run()` method to instantiate this class")

    @abstractmethod
    def setup_fit_predict(self) -> pd.DataFrame:
        raise NotImplementedError("You must implement a `setup_fit_predict` to instantiate this class")

    @abstractmethod
    def detect_anomalies(self) -> pd.DataFrame:
        raise NotImplementedError("You must implement a `detect_anomalies` method to instantiate this class")

    def preprocess(self, time_series_df: pd.DataFrame) -> pd.DataFrame:
        """Eliminates measurements that are labelled as skipped by users."""
        time_series_df = time_series_df.sort_values(by="ds", ascending=True)
        time_series_df = time_series_df.reset_index(drop=True)
        columns = time_series_df.columns
        # Handle if anomaly is detected correctly with the feedback
        if "is_correctly_classified_anomaly" in columns:
            time_series_df.loc[time_series_df["is_correctly_classified_anomaly"] == True, "y"] = None

        if "skipMeasurements" in columns:
            time_series_df = self.handle_skip_measurements(time_series_df)

        available_regressor_columns = [col for col in columns if col in EXTERNAL_REGRESSOR_COLUMNS]
        filtered_columns = ["ds", "y"] + available_regressor_columns
        time_series_df = time_series_df[filtered_columns].reset_index(drop=True)
        return time_series_df

    def handle_skip_measurements(self, time_series_df: pd.DataFrame) -> pd.DataFrame:
        skip_measurements = time_series_df["skipMeasurements"].tolist()
        # Handle previousAndThis
        if "previousAndThis" in skip_measurements:
            last_occurence_previous_and_this = time_series_df[
                time_series_df["skipMeasurements"] == "previousAndThis"
            ].index.max()
            # Check if the last occurrence is the last row of the DataFrame
            if last_occurence_previous_and_this == time_series_df.index[-1]:
                time_series_df = pd.DataFrame(columns=time_series_df.columns)  # Empty DataFrame with same columns
            else:
                time_series_df = time_series_df.iloc[last_occurence_previous_and_this + 1 :]

        # Handle previous
        if "previous" in skip_measurements:
            last_occurence_previous = time_series_df[time_series_df["skipMeasurements"] == "previous"].index.max()
            time_series_df = time_series_df.iloc[last_occurence_previous:]

        # Handle this
        if "this" in skip_measurements:
            # Set y to NaN if we skip this measurement, it is the recommended way by the Prophet documentation
            # see https://facebook.github.io/prophet/docs/outliers
            time_series_df.loc[time_series_df["skipMeasurements"] == "this", "y"] = None
        return time_series_df

    def remove_big_gaps_from_time_series(
        self, time_series_df: pd.DataFrame, freq_detection_result: FreqDetectionResult
    ) -> pd.DataFrame:
        """
        If there are big gaps in the time series due to the missing values, detect that and remove
        the data points preceding the gap. So that, the training data is not biased by the missing values.
        """

        df = time_series_df.copy()
        df = df.sort_values(by="ds", ascending=True)

        non_null_n_rows = df["y"].dropna().shape[0]
        gap_limit = 10
        if non_null_n_rows < 20:
            gap_limit = gap_limit // 3
        elif non_null_n_rows < 40:
            gap_limit = gap_limit // 2

        consequtive_null_mask_list = df["y"].isnull().tolist()
        consequtive_null_counter = 0
        df["_offset"] = [
            consequtive_null_counter := 0 if not i else min((consequtive_null_counter + 1), gap_limit)
            for i in consequtive_null_mask_list
        ]
        # compute the cutoff date by setting the last date where we see 10 nulls in a row
        cutoff_date = df["ds"][df["_offset"] == gap_limit].max()
        if not pd.isna(cutoff_date):
            # remove all data after the cutoff date
            df = df.loc[df["ds"] > cutoff_date]
            self.logs.warning(
                f"Anomaly Detection Training Data Warning: All data points preceding "
                f" '{cutoff_date}' have been excluded from the training dataset. "
                f" This action was necessary due to the identification of more than {gap_limit} consecutive"
                " missing values, which is inconsistent with the detected "
                f"({freq_detection_result.inferred_frequency}) frequency of the dataset."
            )
        df = df.drop(columns="_offset", axis=1)
        df = df.reset_index(drop=True)
        return df
