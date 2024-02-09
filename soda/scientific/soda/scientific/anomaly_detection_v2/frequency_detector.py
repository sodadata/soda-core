from __future__ import annotations

from typing import Any

import pandas as pd
from soda.common.logs import Logs
from soda.scientific.anomaly_detection_v2.globals import (
    DETECTOR_MESSAGES,
    MANUAL_FREQUENCY_MAPPING,
)
from soda.scientific.anomaly_detection_v2.pydantic_models import FreqDetectionResult
from soda.scientific.anomaly_detection_v2.utils import (
    get_not_enough_measurements_freq_result,
)


class FrequencyDetector:
    def __init__(self, logs: Logs, params: dict[str, Any], time_series_df: pd.DataFrame, manual_freq: str = "auto"):
        self.logs = logs
        self.params = params
        self.time_series_df = time_series_df
        self.manual_freq = manual_freq

    def detect_frequency(self) -> FreqDetectionResult:
        min_n_points = self.params["prophet_detector"]["preprocess_params"]["min_number_of_data_points"]
        if not len(self.time_series_df) >= min_n_points:
            return get_not_enough_measurements_freq_result(n_data_points=len(self.time_series_df))

        if self.manual_freq != "auto":
            return FreqDetectionResult(
                inferred_frequency=self.manual_freq,
                df=self.time_series_df,
                freq_detection_strategy="manual_freq",
                error_code_int=DETECTOR_MESSAGES["manual_freq"].error_code_int,
                error_code=DETECTOR_MESSAGES["manual_freq"].error_code_str,
                error_severity=DETECTOR_MESSAGES["manual_freq"].severity,
                error_message=DETECTOR_MESSAGES["manual_freq"].log_message.format(
                    frequency=MANUAL_FREQUENCY_MAPPING.get(self.manual_freq, self.manual_freq)
                ),
            )
        self.logs.debug("Anomaly Detection: Frequency is set to 'auto' and will be detected automatically")
        _df = self.time_series_df.copy()
        _df["ds"] = _df["ds"].dt.tz_localize(None)
        _df = _df.set_index("ds")
        _df = _df.sort_index()
        inferred_frequency = pd.infer_freq(_df.index)
        if inferred_frequency and isinstance(_df, pd.DataFrame):
            self.logs.info(DETECTOR_MESSAGES["native_freq"].log_message)
            return FreqDetectionResult(
                inferred_frequency=inferred_frequency,
                df=_df.tz_localize(None).reset_index(),
                freq_detection_strategy="native_freq",
                error_code_int=DETECTOR_MESSAGES["native_freq"].error_code_int,
                error_code=DETECTOR_MESSAGES["native_freq"].error_code_str,
                error_severity=DETECTOR_MESSAGES["native_freq"].severity,
                error_message=DETECTOR_MESSAGES["native_freq"].log_message,
            )
        #   # if FAILED:
        #       # is it in fact a "daily dataset"?
        #           # chuck time info
        #           # get unique dates, if dupes it's not a daily if not "it is daily".
        #           # impute/fill missing dates + values via interpolation
        #           # make sure we can have a count of the number of the we're about to impute.
        #           # if below rejection threshold, make it a TS and run with it.
        #           # capture a warning and push it into the results.
        _df = _df.reset_index()
        _df["ds"] = _df["ds"].dt.normalize()
        has_dupe_dates = _df.duplicated(subset=["ds"]).any()
        if not has_dupe_dates:
            self.logs.info("Anomaly Detection Frequency Warning: Converted into daily dataset with no data dropping")
            return FreqDetectionResult(
                inferred_frequency="D",
                df=_df,
                freq_detection_strategy="converted_daily_no_dupes",
                error_code_int=DETECTOR_MESSAGES["native_freq"].error_code_int,
                error_code=DETECTOR_MESSAGES["converted_daily_no_dupes"].error_code_str,
                error_severity=DETECTOR_MESSAGES["converted_daily_no_dupes"].severity,
                error_message=DETECTOR_MESSAGES["converted_daily_no_dupes"].log_message,
            )

        #       # if not a near daily, then it's more frequent and we cannot chuck the time
        #       # since we did not get a freq before we know we're still stuffed.
        #           # we either make it be daily (this is the current solution --but I really don't like it)
        is_assume_daily = self.params["prophet_detector"]["preprocess_params"].get("assume_daily", False)
        if is_assume_daily:
            original_number_of_points = len(_df)
            _df = _df.drop_duplicates("ds", keep="last")
            if isinstance(_df, pd.DataFrame):
                self.logs.warning(
                    "Anomaly Detection Frequency Warning: Coerced into daily dataset with last daily time point kept"
                )
                if len(_df) >= min_n_points:
                    return FreqDetectionResult(
                        inferred_frequency="D",
                        df=_df,
                        freq_detection_strategy="coerced_daily",
                        error_code_int=DETECTOR_MESSAGES["native_freq"].error_code_int,
                        error_code=DETECTOR_MESSAGES["coerced_daily"].error_code_str,
                        error_severity=DETECTOR_MESSAGES["coerced_daily"].severity,
                        error_message=DETECTOR_MESSAGES["coerced_daily"].log_message,
                    )
                else:
                    dummy_value = 0
                    freq_result = get_not_enough_measurements_freq_result(n_data_points=dummy_value)
                    # Override error message to make it more informative
                    freq_result.error_message = (
                        f"Anomaly Detection Insufficient Training Data Warning: "
                        "Due to the aggregation of the historical check results into daily frequency, "
                        f"{original_number_of_points} data points were reduced to {len(_df)} data points."
                        " The model requires a minimum of 4 historical measurements."
                    )
                    return freq_result
        # we take the last 4 data points. Try to get a freq on that.
        _df = _df.set_index("ds")
        _df = _df.sort_index()
        inferred_frequency = pd.infer_freq(_df[-4:])
        _df = _df.reset_index()
        if inferred_frequency and isinstance(_df, pd.DataFrame):
            self.logs.warning(
                "Anomaly Detection Frequency Warning: Using inferred frequency from the last 4 data points."
            )
            return FreqDetectionResult(
                inferred_frequency=inferred_frequency,
                df=_df,
                freq_detection_strategy="last_four",
                error_code_int=DETECTOR_MESSAGES["native_freq"].error_code_int,
                error_code=DETECTOR_MESSAGES["last_four"].error_code_str,
                error_severity=DETECTOR_MESSAGES["last_four"].severity,
                error_message=DETECTOR_MESSAGES["last_four"].log_message,
            )
        #           # if we get it:
        #               # make it be the freq of the df, fill missing dates and values and run with it.
        #               # do we want then to run ADS only from those measurements? How do we keep track of that?
        #               # how do we communcate this to our users? Is it even a good idea to do that at all?
        return FreqDetectionResult(
            inferred_frequency=None,
            df=pd.DataFrame(),
            freq_detection_strategy="bailing_out",
            error_code_int=DETECTOR_MESSAGES["bailing_out"].error_code_int,
            error_code=DETECTOR_MESSAGES["bailing_out"].error_code_str,
            error_severity=DETECTOR_MESSAGES["bailing_out"].severity,
            error_message=DETECTOR_MESSAGES["bailing_out"].log_message,
        )
