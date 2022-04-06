from __future__ import annotations

from soda.execution.metric import Metric
from soda.execution.metric_check import MetricCheck
from soda.soda_cloud.historic_descriptor import (
    HistoricCheckResultsDescriptor,
    HistoricMeasurementsDescriptor,
)
from soda.sodacl.metric_check_cfg import MetricCheckCfg

KEY_HISTORIC_MEASUREMENTS = "historic_measurements"
KEY_HISTORIC_CHECK_RESULTS = "historic_check_results"


class AnomalyMetricCheck(MetricCheck):
    def __init__(
        self,
        check_cfg: MetricCheckCfg,
        data_source_scan: DataSourceScan,
        partition: Partition | None = None,
        column: Column | None = None,
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=column,
        )

        metric_check_cfg: MetricCheckCfg = self.check_cfg
        metric_name = metric_check_cfg.metric_name
        metric = self.metrics[metric_name]

        self.historic_descriptors[KEY_HISTORIC_MEASUREMENTS] = HistoricMeasurementsDescriptor(
            metric_identity=metric.identity,
            limit=1000,
        )
        self.historic_descriptors[KEY_HISTORIC_CHECK_RESULTS] = HistoricCheckResultsDescriptor(
            check_identity=self.identity, limit=3
        )

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        # TODO Review the data structure and see if we still need the KEY_HISTORIC_*
        historic_measurements = historic_values.get(KEY_HISTORIC_MEASUREMENTS).get("measurements")
        historic_check_results = historic_values.get(KEY_HISTORIC_CHECK_RESULTS).get("check_results")

        if historic_measurements:
            self.check_value = self.compute_anomaly_score(historic_measurements, historic_check_results)
            self.set_outcome_based_on_check_value()

            # put all diagnostics into a member field like this:
            self.anomaly_values = {}

        else:
            self.logs.warning("Skipping metric check eval because there is not enough historic data yet")

    def compute_anomaly_score(self, measurements, check_results) -> float:
        # TODO test for module installation and set check status to skipped if the module is not installed
        from soda.scientific.anomaly_detection.anomaly_detector import AnomalyDetector

        level, diagnostics = AnomalyDetector(measurements, check_results).evaluate()
        assert isinstance(diagnostics, dict), f"Anomaly diagnostics should be a dict. Got a {type(diagnostics)} instead"
        assert isinstance(
            diagnostics["anomalyProbability"], float
        ), f"Anomaly probability must be a float but it is {type(diagnostics['anomalyProbability'])}"
        anomaly_probability = diagnostics["anomalyProbability"]

        # Diagnostics looks like this:
        {
            "value": 99.0,
            "fail": {"greaterThanOrEqual": 99.00000029005, "lessThanOrEqual": 98.99999968585},
            "warn": {"greaterThanOrEqual": 99.0000002397, "lessThanOrEqual": 98.9999997362},
            "anomalyProbability": 0.0,
            "anomalyPredictedValue": 99.0,
            "anomalyErrorSeverity": "warn",
            "anomalyErrorCode": "made_daily_keeping_last_point_only",
            "feedback": {
                "isCorrectlyClassified": None,
                "isAnomaly": None,
                "reason": None,
                "freeTextReason": None,
                "skipMeasurements": None,
            },
        }

        return anomaly_probability

    def get_cloud_diagnostics_dict(self) -> dict:
        # FIXME: this will probably have to change a bit. The `get_cloud_diagnostics_dict` works based on self.check_value
        # however the diagnostics for ADS are a different breed and returned by AnomalyDetector.evaluate this object should be
        # passed through to cloud.

        # also, the level which will be pass, fail, warn should be accessed from the ADS result
        # not derived by core based on the check value as it is a little bit more complicated.
        cloud_diagnostics = super().get_cloud_diagnostics_dict()
        if self.historic_diff_values:
            cloud_diagnostics["anomaly_values"] = self.anomaly_values

    def get_log_diagnostic_dict(self) -> dict:
        log_diagnostics = super().get_log_diagnostic_dict()
        if self.historic_diff_values:
            log_diagnostics.update(self.anomaly_values)
        return log_diagnostics
