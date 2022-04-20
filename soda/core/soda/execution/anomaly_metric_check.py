from __future__ import annotations

from soda.execution.check_outcome import CheckOutcome
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
        self.diagnostics = {}
        self.cloud_check_type = "anomalyDetection"

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        # TODO Review the data structure and see if we still need the KEY_HISTORIC_*
        historic_measurements = historic_values.get(KEY_HISTORIC_MEASUREMENTS).get("measurements")
        historic_check_results = historic_values.get(KEY_HISTORIC_CHECK_RESULTS).get("check_results")

        if historic_measurements:
            # TODO test for module installation and set check status to skipped if the module is not installed
            from soda.scientific.anomaly_detection.anomaly_detector import (
                AnomalyDetector,
            )

            level, diagnostics = AnomalyDetector(historic_measurements, historic_check_results).evaluate()
            assert isinstance(
                diagnostics, dict
            ), f"Anomaly diagnostics should be a dict. Got a {type(diagnostics)} instead"
            assert isinstance(
                diagnostics["anomalyProbability"], float
            ), f"Anomaly probability must be a float but it is {type(diagnostics['anomalyProbability'])}"

            self.check_value = diagnostics["anomalyProbability"]
            self.outcome = CheckOutcome(level)
            self.diagnostics = diagnostics

        else:
            self.logs.warning("Skipping metric check eval because there is not enough historic data yet")

    def get_cloud_diagnostics_dict(self) -> dict:
        cloud_diagnostics = super().get_cloud_diagnostics_dict()
        cloud_diagnostics["diagnostics"] = self.diagnostics
        return cloud_diagnostics

    def get_log_diagnostic_dict(self) -> dict:
        log_diagnostics = super().get_log_diagnostic_dict()
        if self.historic_diff_values:
            log_diagnostics.update(self.diagnostics)
        return log_diagnostics
