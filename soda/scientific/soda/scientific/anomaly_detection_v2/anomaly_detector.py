from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

import pandas as pd
import yaml
from soda.common.logs import Logs
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    ModelConfigs,
    SeverityLevelParameters,
    TrainingDatasetParameters,
)

from soda.scientific.anomaly_detection_v2.feedback_processor import FeedbackProcessor
from soda.scientific.anomaly_detection_v2.models.prophet_model import ProphetDetector
from soda.scientific.anomaly_detection_v2.pydantic_models import (
    AnomalyDiagnostics,
    AnomalyHistoricalCheckResults,
    AnomalyHistoricalMeasurements,
    AnomalyResult,
    FreqDetectionResult,
)


class AnomalyDetector:
    def __init__(
        self,
        measurements: Dict[str, List[Dict[str, Any]]],
        check_results: Dict[str, List[Dict[str, Any]]],
        logs: Logs,
        model_cfg: ModelConfigs,
        training_dataset_params: TrainingDatasetParameters,
        severity_level_params: SeverityLevelParameters,
    ):
        self._logs = logs
        self.measurements = measurements
        self.check_results = check_results
        self.model_cfg = model_cfg
        self.training_dataset_params = training_dataset_params
        self.severity_level_params = severity_level_params
        self.params = self._parse_params()

    def evaluate(self) -> Tuple[str, Dict[str, Any]]:
        df_historic = self._generate_historical_ad_df()

        feedback = FeedbackProcessor(params=self.params, df_historic=df_historic, logs=self._logs)
        has_exogenous_regressor, feedback_processed_df = feedback.get_processed_feedback_df()

        detector = ProphetDetector(
            logs=self._logs,
            params=self.params,
            time_series_df=feedback_processed_df,
            model_cfg=self.model_cfg,
            training_dataset_params=self.training_dataset_params,
            severity_level_params=self.severity_level_params,
            has_exogenous_regressor=has_exogenous_regressor,
        )
        df_anomalies, freq_detection_result = detector.run()

        level, diagnostics = self._parse_output(df_anomalies, freq_detection_result)

        return level, diagnostics

    def _parse_historical_measurements(self) -> pd.DataFrame:
        if self.measurements:
            parsed_measurements = AnomalyHistoricalMeasurements.model_validate(self.measurements)
            _df_measurements = pd.DataFrame.from_dict(parsed_measurements.model_dump()["results"])
            return _df_measurements
        else:
            raise ValueError("No historical measurements found.")

    def _parse_historical_check_results(self) -> pd.DataFrame:
        if self.check_results.get("results"):
            parsed_check_results = AnomalyHistoricalCheckResults.model_validate(self.check_results)
            _df_check_results = pd.DataFrame.from_dict(parsed_check_results.model_dump()["results"])
            return _df_check_results
        else:
            self._logs.debug(
                "No past check results found. This could be because there are no past runs of "
                "Anomaly Detection for this check yet."
            )
            parsed_check_results = AnomalyHistoricalCheckResults(results=[AnomalyResult()])
            _df_check_results = pd.DataFrame.from_dict(parsed_check_results.model_dump()["results"])
            return _df_check_results

    def _generate_historical_ad_df(self) -> pd.DataFrame:
        df_measurements = self._parse_historical_measurements()
        df_check_results = self._parse_historical_check_results()

        self._logs.debug("Got test results from data request. Merging it with the measurements")
        df_historical = df_measurements.merge(
            df_check_results,
            how="left",
            left_on="id",
            right_on="measurementId",
            suffixes=("", "_tr"),
        )

        # Flatten diagnostics
        df_historical["diagnostics"] = df_historical["diagnostics"].apply(lambda x: {} if pd.isnull(x) else x)
        df_diagnostics_flattened = pd.DataFrame(df_historical["diagnostics"].tolist())
        df_historical_flattened = pd.merge(
            df_historical,
            df_diagnostics_flattened,
            left_index=True,
            right_index=True,
            suffixes=("", "_diag"),
        )

        column_maps = self.params["request_params"]["columns_mapping"]

        # Filter out columns that are not in the target_columns list
        target_columns = list(column_maps.keys())
        selected_columns = [col for col in df_historical_flattened.columns if col in target_columns]
        df_historical_flattened = df_historical_flattened[selected_columns]
        df_historical_flattened = df_historical_flattened.rename(columns=column_maps)
        df_historical_flattened["ds"] = pd.to_datetime(df_historical_flattened["ds"])
        df_historical_flattened["ds"] = df_historical_flattened["ds"].dt.tz_localize(None)
        return df_historical_flattened

    def _parse_params(self) -> Dict[str, Any]:
        try:
            this_dir = Path(__file__).parent.resolve()
            config_file = this_dir.joinpath("detector_config.yaml")
            # Read detector configuration
            with open(config_file) as stream:
                loaded_config = yaml.safe_load(stream)

            # Manipulate configuration
            loaded_config["response_params"]["output_columns"] = self._replace_none_values_by_key(
                loaded_config["response_params"]["output_columns"]
            )
            loaded_config["feedback_processor_params"]["output_columns"] = self._replace_none_values_by_key(
                loaded_config["feedback_processor_params"]["output_columns"]
            )
            self._logs.debug(f"Anomaly Detection: config parsed {loaded_config}")

            return loaded_config

        except Exception as e:
            self._logs.error(e)
            raise e

    @staticmethod
    def _replace_none_values_by_key(dct: Dict[str, Any]) -> Mapping[str, Any]:
        result = {}
        for key, value in dct.items():
            if value is None:
                value = key
            result[key] = value
        return result

    @staticmethod
    def _parse_output(
        df_anomalies: pd.DataFrame, freq_detection_result: FreqDetectionResult
    ) -> Tuple[str, Dict[str, Any]]:
        if not df_anomalies.empty:
            results_dict = df_anomalies.to_dict(orient="records")[0]
            level = results_dict["level"]
            diagnostics = {
                "value": results_dict["real_data"],
                "warn": {
                    "greaterThanOrEqual": results_dict["warning_greater_than_or_equal"],
                    "lessThanOrEqual": results_dict["warning_lower_than_or_equal"],
                },
                "fail": {
                    "greaterThanOrEqual": results_dict["critical_greater_than_or_equal"],
                    "lessThanOrEqual": results_dict["critical_lower_than_or_equal"],
                },
                "anomalyPredictedValue": results_dict["yhat"],
                "anomalyErrorSeverity": freq_detection_result.error_severity,
                "anomalyErrorCode": freq_detection_result.error_code,
                "anomalyErrorMessage": freq_detection_result.error_message,
            }
        else:
            level = "pass"
            diagnostics = {
                "value": None,
                "warn": None,
                "fail": None,
                "anomalyProbability": None,
                "anomalyPredictedValue": None,
                "anomalyErrorSeverity": freq_detection_result.error_severity,
                "anomalyErrorCode": freq_detection_result.error_code,
                "amomalyErrorMessage": freq_detection_result.error_message,
            }

        diagnostics_dict: Dict[str, Any] = AnomalyDiagnostics.model_validate(diagnostics).model_dump()
        return level, diagnostics_dict
