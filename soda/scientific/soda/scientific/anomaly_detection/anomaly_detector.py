import datetime
from typing import Any, Dict, List, Mapping, Optional, Union
import pandas as pd
from pathlib import Path
import logging
from pydantic import BaseModel, validator
import yaml

from soda.scientific.anomaly_detection.feedback_processor import FeedbackProcessor
from soda.scientific.anomaly_detection.models.prophet_model import (
    ProphetDetector,
    FreqDetectionResult,
)


class UserFeedback(BaseModel):
    """Validation model for user feedback data dict in payload."""

    isCorrectlyClassified: Optional[bool]
    isAnomaly: Optional[bool]
    reason: Optional[str]
    freeTextReason: Optional[str]
    skipMeasurements: Optional[str]

    @validator("skipMeasurements")
    def check_accepted_values_skip_measurements(cls, v):
        accepted_values = ["this", "previous", "previousAndThis", None]
        assert (
            v in accepted_values
        ), f"skip_measurements must be one of {accepted_values}, but '{v}' was provided."
        return v


class SeverityLevelAreas(BaseModel):
    """Validates severity levels dicts."""

    greaterThanOrEqual: Optional[float]
    lessThanOrEqual: Optional[float]


class AnomalyDiagnostics(BaseModel):
    value: float
    fail: Optional[SeverityLevelAreas]
    warn: Optional[SeverityLevelAreas]
    anomalyProbability: Optional[float]
    anomalyPredictedValue: Optional[float]
    anomalyErrorSeverity: str = "pass"
    anomalyErrorCode: str = ""


class LocationModel(BaseModel):
    filePath: str
    line: int
    col: int


# some of those fields might end up being ignored down the line by ADS
class AnomalyResult(BaseModel):
    identity: str
    measurementId: str
    type: str
    definition: str
    location: LocationModel
    metrics: List[str]
    dataSource: str
    table: str
    partition: str
    column: str
    outcome: str
    diagnostics: AnomalyDiagnostics


class AnomalyHistoricalCheckResults(BaseModel):
    results: List[AnomalyResult]


class AnomalyHistoricalMeasurement(BaseModel):
    identity: str
    id: str
    value: float
    data_time: datetime.datetime


class AnomalyHistoricalMeasurements(BaseModel):
    results: List[AnomalyHistoricalMeasurement]


MOCK_ANOMALY_DIAGNOSTICS = [
    {
        "value": 12.2,
        "fail": {
            "lessThanOrEqual": 12,
            "greaterThanOrEqual": 12,
        },
        "warn": {
            "lessThanOrEqual": 12,
            "greaterThanOrEqual": 12,
        },
        "anomalyProbability": 0.1,
        "anomalyPredictedValue": 13,
        "anomalyErrorSeverity": "warn",
        "anomalyErrorCode": "inferred_daily_frequency",
        "feedback": {},
    }
]


class AnomalyDetector:
    def __init__(self, measurements, check_results):
        self.df_measurements = self._get_historical_measurements(measurements)
        self.df_check_results = self._get_historical_check_results(check_results)
        self.params = self._parse_params()

    @staticmethod
    def _get_historical_measurements(measurements: List[Dict[str, Any]]) -> pd.DataFrame:
        if measurements:
            parsed_measurements = AnomalyHistoricalMeasurements.parse_obj({"results": measurements})
            _df_measurements = pd.DataFrame.from_dict(parsed_measurements.dict()["results"])
            return _df_measurements
        else:
            return None

    @staticmethod
    def _get_historical_check_results(
        check_results: Dict[str, List[Dict[str, Any]]]
    ) -> pd.DataFrame:
        if check_results:
            parsed_check_results = AnomalyHistoricalCheckResults.parse_obj(check_results)
            _df_check_results = pd.DataFrame.from_dict(parsed_check_results.dict()["results"])
            return _df_check_results
        else:
            return None

    def _convert_to_well_shaped_df(self) -> pd.DataFrame:
        if not self.df_check_results.empty:
            logging.info("Got test results from data request. Merging it with the measurements")
            df = self.df_measurements.merge(
                self.df_check_results,
                how="left",
                left_on="id",
                right_on="measurementId",
                suffixes=("", "_tr"),
            )
        else:
            df = self.df_measurements.copy()

        # Flatten diagnostics dictionary
        df_flattened = self.flatten_df(df.copy(), "diagnostics")

        column_maps = self.params["request_params"]["columns_mapping"]
        df_flattened = df_flattened[
            df_flattened.columns[df_flattened.columns.isin(list(column_maps.keys()))]
        ]
        df_flattened = df_flattened.rename(columns=column_maps)  # type: ignore
        df_flattened["ds"] = pd.to_datetime(df_flattened["ds"])  # type: ignore
        df_flattened["ds"] = df_flattened["ds"].dt.tz_localize(None)
        return df_flattened

    @staticmethod
    def flatten_df(df: pd.DataFrame, target_col_name: str) -> pd.DataFrame:
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        df[target_col_name] = df[target_col_name].apply(lambda x: {} if pd.isnull(x) else x)

        target_array_to_flatten = list(df[target_col_name].values)
        df_flattened = pd.DataFrame.from_dict(target_array_to_flatten)  # type: ignore
        df_joined = pd.merge(
            df,
            df_flattened,
            left_index=True,
            right_index=True,
            suffixes=("", "_diag"),
        )
        return df_joined

    def _parse_params(self) -> Dict[str, Any]:
        try:
            this_dir = Path(__file__).parent.resolve()

            # Read detector configuration
            with open(this_dir.joinpath("detector_config.yml")) as stream:
                loaded_config = yaml.safe_load(stream)

            # Manipulate configuration
            loaded_config["response_params"]["output_columns"] = self._replace_none_values_by_key(
                loaded_config["response_params"]["output_columns"]
            )
            loaded_config["feedback_processor_params"][
                "output_columns"
            ] = self._replace_none_values_by_key(
                loaded_config["feedback_processor_params"]["output_columns"]
            )
            logging.info(f"Config parsed {loaded_config}")

            return loaded_config

        except Exception as e:
            logging.error(e)
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
    ) -> Union[str, Dict[str, Any]]:
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
                "anomalyProbability": results_dict["anomaly_probability"],
                "anomalyPredictedValue": results_dict["trend"],
                "anomalyErrorSeverity": freq_detection_result.error_severity,
                "anomalyErrorCode": freq_detection_result.error_code,
            }
        else:
            level = "pass"
            diagnostics = {
                "value": None,
                "warn": None,
                "fail": None,
                "anomalyProbability": None,
                "anomalyPredictedValue": None,
            }

        diagnostics = AnomalyDiagnostics.parse_obj(diagnostics).dict()
        return level, diagnostics

    def evaluate(self):
        df_historic = self._convert_to_well_shaped_df()

        feedback = FeedbackProcessor(params=self.params, df_historic=df_historic)
        feedback.run()

        detector = ProphetDetector(
            params=self.params,
            time_series_data=feedback.df_feedback_processed,
            has_exegonenous_regressor=feedback.has_exegonenous_regressor,
        )
        df_anomalies = detector.run()

        level, diagnostics = self._parse_output(df_anomalies, detector.freq_detection_result)

        return level, diagnostics


if __name__ == "__main__":
    # TODO: remove this, it's here for testing purposes only.
    historical_result_mock = {
        "results": [
            {
                "identity": "c0db6dcb",
                "measurementId": "1",
                "type": "anomalyDetection",
                "definition": "anomaly for table_a",
                "location": {"filePath": "check.yaml", "line": 3, "col": 11},
                "metrics": ["snowflake.table_a.count"],
                "dataSource": "snowflake",
                "table": "table_a",
                "partition": "",
                "column": "",
                "outcome": "pass",
                "diagnostics": {
                    "value": 12.2,
                    "fail": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "warn": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "anomalyProbability": 0.1,
                    "anomalyPredictedValue": 13,
                    "anomalyErrorSeverity": "warn",
                    "anomalyErrorCode": "inferred_daily_frequency",
                    "feedback": {},
                },
            },
            {
                "identity": "c0db6dcb",
                "measurementId": "2",
                "type": "anomalyDetection",
                "definition": "anomaly for table_a",
                "location": {"filePath": "check.yaml", "line": 3, "col": 11},
                "metrics": ["snowflake.table_a.count"],
                "dataSource": "snowflake",
                "table": "table_a",
                "partition": "",
                "column": "",
                "outcome": "pass",
                "diagnostics": {
                    "value": 12.2,
                    "fail": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "warn": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "anomalyProbability": 0.1,
                    "anomalyPredictedValue": 13,
                    "anomalyErrorSeverity": "warn",
                    "anomalyErrorCode": "inferred_daily_frequency",
                    "feedback": {},
                },
            },
            {
                "identity": "c0db6dcb",
                "measurementId": "3",
                "type": "anomalyDetection",
                "definition": "anomaly for table_a",
                "location": {"filePath": "check.yaml", "line": 3, "col": 11},
                "metrics": ["snowflake.table_a.count"],
                "dataSource": "snowflake",
                "table": "table_a",
                "partition": "",
                "column": "",
                "outcome": "pass",
                "diagnostics": {
                    "value": 12.2,
                    "fail": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "warn": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "anomalyProbability": 0.1,
                    "anomalyPredictedValue": 13,
                    "anomalyErrorSeverity": "warn",
                    "anomalyErrorCode": "inferred_daily_frequency",
                },
            },
            {
                "identity": "c0db6dcb",
                "measurementId": "4",
                "type": "anomalyDetection",
                "definition": "anomaly for table_a",
                "location": {"filePath": "check.yaml", "line": 3, "col": 11},
                "metrics": ["snowflake.table_a.count"],
                "dataSource": "snowflake",
                "table": "table_a",
                "partition": "",
                "column": "",
                "outcome": "pass",
                "diagnostics": {
                    "value": 12.2,
                    "fail": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "warn": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "anomalyProbability": 0.1,
                    "anomalyPredictedValue": 13,
                    "anomalyErrorSeverity": "warn",
                    "anomalyErrorCode": "inferred_daily_frequency",
                    "feedback": {
                        "isAnomaly": False,
                        "reason": "dymmy reason",
                        "freeTextreason": "some human text",
                    },
                },
            },
            {
                "identity": "c0db6dcb",
                "measurementId": "5",
                "type": "anomalyDetection",
                "definition": "anomaly for table_a",
                "location": {"filePath": "check.yaml", "line": 3, "col": 11},
                "metrics": ["snowflake.table_a.count"],
                "dataSource": "snowflake",
                "table": "table_a",
                "partition": "",
                "column": "",
                "outcome": "pass",
                "diagnostics": {
                    "value": 12.2,
                    "fail": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "warn": {
                        "lessThanOrEqual": 12,
                        "greaterThanOrEqual": 12,
                    },
                    "anomalyProbability": 0.1,
                    "anomalyPredictedValue": 13,
                    "anomalyErrorSeverity": "warn",
                    "anomalyErrorCode": "inferred_daily_frequency",
                    "feedback": {
                        "isCorrectlyClassified": False,
                        "isAnomaly": False,
                        "reason": "dymmy reason",
                        "freeTextReason": "some human text",
                    },
                },
            },
        ]
    }

    historical_measurement_mock = {
        "results": [
            {
                "identity": "c0db6dcb",
                "id": "1",
                "value": 12.2,
                "dataTime": "2022-01-01 00:00:00",
            },
            {
                "identity": "c0db6dcb",
                "id": "2",
                "value": 12.6,
                "dataTime": "2022-01-02 00:00:00",
            },
            {
                "identity": "c0db6dcb",
                "id": "3",
                "value": 12.9,
                "dataTime": "2022-01-03 00:00:00",
            },
            {
                "identity": "c0db6dcb",
                "id": "4",
                "value": 14,
                "dataTime": "2022-01-04 00:00:00",
            },
            {
                "identity": "c0db6dcb",
                "id": "5",
                "value": 50,
                "dataTime": "2022-01-05 00:00:00",
            },
        ]
    }

    detector = AnomalyDetector(
        measurements=historical_measurement_mock, check_results=historical_result_mock
    )
    outcome, diagnostics = detector.evaluate()
