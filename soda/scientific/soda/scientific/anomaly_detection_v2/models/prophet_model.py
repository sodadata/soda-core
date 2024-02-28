from __future__ import annotations

import ast
import itertools
import logging
import multiprocessing
import random
import sys
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
from prophet.diagnostics import cross_validation, performance_metrics
from soda.common.logs import Logs
from soda.execution.check.anomaly_detection_metric_check import HISTORIC_RESULTS_LIMIT
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    ModelConfigs,
    ProphetDefaultHyperparameters,
    SeverityLevelParameters,
    TrainingDatasetParameters,
)
from tqdm import tqdm

from soda.scientific.anomaly_detection_v2.exceptions import (
    AggregationValueError,
    FreqDetectionResultError,
    NotSupportedHolidayCountryError,
    WindowLengthError,
)
from soda.scientific.anomaly_detection_v2.frequency_detector import FrequencyDetector
from soda.scientific.anomaly_detection_v2.globals import ERROR_CODE_LEVEL_CUTTOFF
from soda.scientific.anomaly_detection_v2.models.base import BaseDetector
from soda.scientific.anomaly_detection_v2.pydantic_models import FreqDetectionResult
from soda.scientific.anomaly_detection_v2.utils import (
    SuppressStdoutStderr,
    get_not_enough_measurements_freq_result,
)

with SuppressStdoutStderr():
    from prophet import Prophet


class ProphetDetector(BaseDetector):
    """ProphetDetector."""

    def __init__(
        self,
        logs: Logs,
        params: Dict[str, Any],
        time_series_df: pd.DataFrame,
        model_cfg: ModelConfigs,
        training_dataset_params: TrainingDatasetParameters,
        severity_level_params: SeverityLevelParameters,
        has_exogenous_regressor: bool = False,
    ) -> None:
        """Constructor for ProphetDetector

        Args:
            params (Dict[str, Any]): config class parsed from detector_config.yml.
            time_series_df (pd.DataFrame): time series data to be used for training and prediction.
            logs (Logs): logging object.
            model_cfg (ModelConfigs): hyperparameter configs.
            training_dataset_params (TrainingDatasetParameters): training dataset configs.
            severity_level_params (SeverityLevelParameters): severity level configs.
            has_exogenous_regressor (bool, optional): whether the time series data has an exogenous regressor. Defaults to False.

        Returns:
            None
        """
        super().__init__(
            logs=logs, time_series_df=time_series_df
        )  # runs the measurement elimination that is contained in the base

        try:
            if "pytest" not in sys.argv[0]:
                multiprocessing.set_start_method("fork")
        except:
            pass

        self.logs = logs
        self.params = params
        self.raw_time_series_df = time_series_df
        self.model_cfg = model_cfg
        self.hyperparamaters_cfg = model_cfg.hyperparameters
        self.training_dataset_params = training_dataset_params
        self.severity_level_params = severity_level_params
        self.has_exogenous_regressor = has_exogenous_regressor

        self._prophet_detector_params = self.params["prophet_detector"]
        self._min_n_points = self._prophet_detector_params["preprocess_params"]["min_number_of_data_points"]
        self._anomaly_detection_params = self._prophet_detector_params["anomaly_detection"]
        self._is_trained: bool = False

    def run(self) -> Tuple[pd.DataFrame, FreqDetectionResult]:
        """Convenience orchestrator that outputs last anomalies as a pd.DataFrame."""
        try:
            if self._prophet_detector_params["suppress_stan"]:
                pd.set_option("mode.chained_assignment", None)

            # Skip measurements based on feedbacks given from SODA Cloud
            preprocessed_df = self.preprocess(time_series_df=self.raw_time_series_df)

            # Automatically detect frequency of the time series
            freq_detector = FrequencyDetector(
                logs=self.logs,
                params=self.params,
                time_series_df=preprocessed_df,
                manual_freq=self.training_dataset_params.frequency,
            )
            freq_detection_result = freq_detector.detect_frequency()

            # Return if frequency detection failed
            if freq_detection_result.error_code_int >= ERROR_CODE_LEVEL_CUTTOFF:
                return self.exit_with_warning(freq_detection_result)

            # Apply training dataset configurations
            training_df = self.apply_training_dataset_configs(
                time_series_df=preprocessed_df, freq_detection_result=freq_detection_result
            )

            # Remove big gaps from the time series to not confuse Prophet
            training_df = self.remove_big_gaps_from_time_series(
                time_series_df=training_df, freq_detection_result=freq_detection_result
            )

            # Only use the last n points for training based on the window length
            window_length = self.get_window_length(training_df=training_df)
            training_df = training_df.iloc[-window_length:]

            training_df_shape = training_df["y"].dropna().shape[0]
            if training_df_shape <= self._min_n_points:
                freq_detection_result = get_not_enough_measurements_freq_result(n_data_points=training_df_shape)
                return self.exit_with_warning(freq_detection_result)

            model_hyperparameters = self.get_prophet_hyperparameters(time_series_df=training_df)

            predictions_df = self.setup_fit_predict(
                time_series_df=training_df, model_hyperparameters=model_hyperparameters
            )
            anomalies_df = self.detect_anomalies(time_series_df=training_df, predictions_df=predictions_df)
            anomalies_df = self.generate_severity_zones(anomalies_df=anomalies_df)
            anomalies_df = self.compute_alert_level(anomalies_df=anomalies_df)
            return anomalies_df, freq_detection_result
        except Exception as e:
            raise e

    def get_window_length(self, training_df: pd.DataFrame) -> int:
        original_window_length = self.training_dataset_params.window_length
        if original_window_length <= self._min_n_points:
            raise WindowLengthError(
                "Anomaly Detection Error: The window_length parameter is too small, "
                f"it is set to {original_window_length} but it should be at least {self._min_n_points}. "
            )
        elif original_window_length > HISTORIC_RESULTS_LIMIT:
            raise WindowLengthError(
                "Anomaly Detection Error: The window_length parameter is too big"
                f" it is set to {original_window_length} but it should be at most {HISTORIC_RESULTS_LIMIT}. "
            )
        adjusted_window_length = min(training_df["y"].dropna().shape[0], self.training_dataset_params.window_length)
        return adjusted_window_length

    def apply_training_dataset_configs(
        self, time_series_df: pd.DataFrame, freq_detection_result: FreqDetectionResult
    ) -> pd.DataFrame:
        df = time_series_df.copy()
        df = df.set_index("ds")
        frequency = freq_detection_result.inferred_frequency
        aggregation_function = self.training_dataset_params.aggregation_function
        try:
            aggregated_df = pd.DataFrame(df.resample(frequency).agg(aggregation_function))
            aggregated_df = aggregated_df.reset_index()
            if "external_regressor" in df.columns:
                aggregated_df["external_regressor"] = aggregated_df["external_regressor"].fillna(value=0)
        except AttributeError:
            raise AggregationValueError(
                f"Anomaly Detection: Aggregation function '{aggregation_function}' is not supported. "
            )
        except ValueError:
            raise FreqDetectionResultError(f"Anomaly Detection: Frequency parameter '{frequency}' is not supported. ")
        except Exception as e:
            raise e
        return aggregated_df

    def exit_with_warning(self, freq_detection_result: FreqDetectionResult) -> Tuple[pd.DataFrame, FreqDetectionResult]:
        self.logs.warning(freq_detection_result.error_message)
        anomalies_df = pd.DataFrame()
        return anomalies_df, freq_detection_result

    def find_best_performed_hyperparameters(
        self, hyperparameter_performances_df: pd.DataFrame
    ) -> ProphetDefaultHyperparameters:
        # enable prophet logging again
        logging.getLogger("prophet").setLevel(logging.INFO)
        sort_by = ["coverage", "smape", "mdape", "rmse", "mse"]
        objective = self.hyperparamaters_cfg.dynamic.objective_metric  # type: ignore

        if objective in sort_by:
            objective_index = sort_by.index(objective)
            # remove objective from sorting metrics and insert it at the beginning
            sort_by.remove(sort_by[objective_index])
        # Add sorting metrics to the beginning of the list
        if isinstance(objective, str):
            objective = [objective]
        sort_by = objective + sort_by
        ascending = [col != "coverage" for col in sort_by]
        best_params = hyperparameter_performances_df.sort_values(by=sort_by, ascending=ascending)["hyperparams"].values[
            0
        ]
        dict_best_params = ast.literal_eval(best_params)

        # Update hyperparameters with best params
        best_hyperparameters = ProphetDefaultHyperparameters(**dict_best_params)
        return best_hyperparameters

    def get_hyperparameters_performance_df(
        self, time_series_df: pd.DataFrame, cutoff_point_for_cv: int
    ) -> pd.DataFrame:
        param_grid = self.hyperparamaters_cfg.dynamic.parameter_grid.model_dump()  # type: ignore
        all_hyperparams = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

        self.logs.info(
            f"Anomaly Detection: Start hyperparameter tuning with grid search having {len(all_hyperparams)} combinations"
        )

        # Set cross validation parameters
        parallelize_cross_validation = self.hyperparamaters_cfg.dynamic.parallelize_cross_validation  # type: ignore
        if parallelize_cross_validation is True:
            parallelize_cross_validation = "processes"

        # find time delta in a new column in a rolling window of 1
        time_delta_series = time_series_df["ds"].diff(periods=1)
        inferred_time_delta = time_delta_series.dropna().iloc[-1]
        df_all_performance = pd.DataFrame()
        for hyperparams in tqdm(all_hyperparams):
            # disable prophet logging
            logging.getLogger("prophet").setLevel(logging.ERROR)
            model = Prophet(**hyperparams).fit(time_series_df)
            df_cv = cross_validation(
                model,
                initial=cutoff_point_for_cv * inferred_time_delta,
                horizon=inferred_time_delta,
                period=inferred_time_delta,
                parallel=parallelize_cross_validation,
            )
            df_performance = pd.DataFrame(performance_metrics(df_cv, rolling_window=1))
            df_performance["hyperparams"] = str(hyperparams)
            df_all_performance = pd.concat([df_all_performance, df_performance])
        return df_all_performance

    def get_prophet_hyperparameters(self, time_series_df: pd.DataFrame) -> ProphetDefaultHyperparameters:
        if self.hyperparamaters_cfg.dynamic is None:
            return self.hyperparamaters_cfg.static.profile.custom_hyperparameters

        # Start tuning the hyperparameters if dynamic is not None
        n_data_points = time_series_df["y"].dropna().shape[0]
        cross_validation_folds = self.hyperparamaters_cfg.dynamic.cross_validation_folds
        cutoff_point_for_cv = n_data_points - cross_validation_folds - 1

        if cutoff_point_for_cv < self._min_n_points:
            self.logs.warning(
                "Anomaly Detection Warning: Hyperparameter tuning is being "
                "skipped due to insufficient data for cross-validation. "
                f"The 'cross_validation_folds' is set to {cross_validation_folds}, but there are "
                f"only {n_data_points} data points available. "
            )
            return self.hyperparamaters_cfg.static.profile.custom_hyperparameters

        hyperparameter_performances_df = self.get_hyperparameters_performance_df(
            time_series_df=time_series_df, cutoff_point_for_cv=cutoff_point_for_cv
        )
        best_hyperparameters = self.find_best_performed_hyperparameters(
            hyperparameter_performances_df=hyperparameter_performances_df
        )

        self.logs.debug(
            "Anomaly Detection: Hyperparameter tuning "
            f"finished with the following best hyperparameters:\n{best_hyperparameters.model_dump_json(indent=4)}"
        )
        return best_hyperparameters

    def setup_fit_predict(
        self, time_series_df: pd.DataFrame, model_hyperparameters: ProphetDefaultHyperparameters
    ) -> pd.DataFrame:
        """Sets up Prophet model and fits it on the self.time_series_df."""

        self.logs.debug(
            f"Anomaly Detection: Fitting prophet model with the following parameters:\n{model_hyperparameters.model_dump_json(indent=4)}"
        )
        model = Prophet(**model_hyperparameters.model_dump())
        holidays_country_code = self.model_cfg.holidays_country_code
        # Add country specific holidays
        if holidays_country_code is not None:
            try:
                model = model.add_country_holidays(country_name=holidays_country_code)
            except AttributeError:
                raise NotSupportedHolidayCountryError(
                    f"Anomaly Detection Error: Country '{holidays_country_code}' is not supported. "
                    "The list of supported countries can be found here: "
                    "https://github.com/vacanza/python-holidays/"
                )
        if "external_regressor" in time_series_df:
            self.logs.info(
                "Anomaly Detection: Found a custom external_regressor derived from user feedback and adding it to Prophet model"
            )
            model = model.add_regressor("external_regressor", mode="multiplicative")
        else:
            self.logs.debug("Anomaly Detection: No external_regressor/user feedback found")
        # Set seed to get reproducible results
        np.random.seed(0)
        random.seed(0)
        if self._prophet_detector_params["suppress_stan"]:
            with SuppressStdoutStderr():
                model.fit(time_series_df.iloc[:-1])
        else:
            model.fit(time_series_df.iloc[:-1])
        predictions_df = model.predict(time_series_df)
        self._is_trained = True
        return predictions_df

    @staticmethod
    def _is_integer(x: Any) -> bool:
        try:
            # This will be True for both integers and floats without a decimal component
            return float(x).is_integer()
        except (ValueError, TypeError):
            # If x cannot be converted to float, it's definitely not an integer
            return False

    def get_upper_and_lower_bounds(self, predictions_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        lower_bound = predictions_df["yhat_lower"]
        upper_bound = predictions_df["yhat_upper"]
        yhat = predictions_df["yhat"]

        # Modify bounds if necessary
        min_ci_ratio = self.severity_level_params.min_confidence_interval_ratio
        minimum_lower_bound = yhat * (1 - min_ci_ratio)
        minimum_upper_bound = yhat * (1 + min_ci_ratio)

        lower_bound = lower_bound.where(lower_bound < minimum_lower_bound, minimum_lower_bound)
        upper_bound = upper_bound.where(upper_bound > minimum_upper_bound, minimum_upper_bound)

        return lower_bound, upper_bound

    def detect_anomalies(self, time_series_df: pd.DataFrame, predictions_df: pd.DataFrame) -> pd.DataFrame:
        n_predicted_anomalies = self._anomaly_detection_params["n_points"]
        predictions_df = predictions_df.iloc[-n_predicted_anomalies:]  # noqa: E203

        # Merge predictions with time_series_df to get the real data
        predictions_df = predictions_df.merge(time_series_df[["ds", "y"]], on="ds", how="left")
        predictions_df = predictions_df.rename(columns={"y": "real_data"})

        self.logs.debug(f"Anomaly Detection: detecting anomalies for the last {n_predicted_anomalies} points.")

        # check whether y value is an integer
        is_real_value_always_integer = time_series_df["y"].dropna().apply(self._is_integer).all()

        # If all values are same like 0.0, then we can't assume that the value is always integer
        # Check whether the values are not always the same
        if is_real_value_always_integer:
            is_real_value_always_integer = time_series_df["y"].dropna().nunique() > 1

        lower_bound, upper_bound = self.get_upper_and_lower_bounds(predictions_df=predictions_df)

        if is_real_value_always_integer:
            predictions_df["yhat_lower"] = np.floor(lower_bound)
            predictions_df["yhat_upper"] = np.ceil(upper_bound)
        else:
            predictions_df["real_data"] = predictions_df["real_data"].round(10)
            predictions_df["yhat_lower"] = lower_bound.round(10)
            predictions_df["yhat_upper"] = upper_bound.round(10)

        # flag data points that fall out of confidence bounds
        predictions_df["is_anomaly"] = 0
        predictions_df.loc[predictions_df["real_data"] > predictions_df["yhat_upper"], "is_anomaly"] = 1
        predictions_df.loc[predictions_df["real_data"] < predictions_df["yhat_lower"], "is_anomaly"] = -1
        return predictions_df

    def generate_severity_zones(self, anomalies_df: pd.DataFrame) -> pd.DataFrame:
        # See criticality_threshold_calc method, the critical zone will always take over and
        # "extend" or replace the extreme to inf points of the warning zone.
        warning_ratio = self.severity_level_params.warning_ratio
        buffer = (anomalies_df["yhat_upper"] - anomalies_df["yhat_lower"]) * warning_ratio
        anomalies_df["critical_greater_than_or_equal"] = anomalies_df["yhat_upper"] + buffer
        anomalies_df["critical_lower_than_or_equal"] = anomalies_df["yhat_lower"] - buffer
        # The bounds for warning are in fact anything that is outside of the model's
        # confidence bounds so we simply reassign them to another column.
        anomalies_df["warning_greater_than_or_equal"] = anomalies_df["yhat_upper"]
        anomalies_df["warning_lower_than_or_equal"] = anomalies_df["yhat_lower"]
        return anomalies_df

    def compute_alert_level(self, anomalies_df: pd.DataFrame) -> pd.DataFrame:
        def determine_level(row: pd.Series) -> str:
            if row["is_anomaly"] != 0:
                if (
                    row["real_data"] <= row["critical_lower_than_or_equal"]
                    or row["real_data"] >= row["critical_greater_than_or_equal"]
                ):
                    return "fail"
                elif (
                    row["real_data"] <= row["warning_lower_than_or_equal"]
                    and row["real_data"] > row["critical_lower_than_or_equal"]
                ) or (
                    row["real_data"] >= row["warning_greater_than_or_equal"]
                    and row["real_data"] < row["critical_greater_than_or_equal"]
                ):
                    return "warn"
            return "pass"

        # Apply the function to each row
        anomalies_df["level"] = anomalies_df.apply(determine_level, axis=1)
        return anomalies_df
