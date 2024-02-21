from __future__ import annotations

from datetime import timezone
from typing import Any

from soda.cloud.historic_descriptor import (
    HistoricCheckResultsDescriptor,
    HistoricMeasurementsDescriptor,
)
from soda.common.exceptions import SODA_SCIENTIFIC_MISSING_LOG_MESSAGE
from soda.execution.check.metric_check import MetricCheck
from soda.execution.check_outcome import CheckOutcome
from soda.execution.column import Column
from soda.execution.data_source_scan import DataSourceScan
from soda.execution.metric.metric import Metric
from soda.execution.partition import Partition
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    AnomalyDetectionMetricCheckCfg,
)

KEY_HISTORIC_MEASUREMENTS = "historic_measurements"
KEY_HISTORIC_CHECK_RESULTS = "historic_check_results"
HISTORIC_RESULTS_LIMIT = 1000


class AnomalyDetectionMetricCheck(MetricCheck):
    def __init__(
        self,
        check_cfg: AnomalyDetectionMetricCheckCfg,
        data_source_scan: DataSourceScan,
        partition: Partition | None = None,
        column: Column | None = None,
    ):
        try:
            super().__init__(
                check_cfg=check_cfg,
                data_source_scan=data_source_scan,
                partition=partition,
                column=column,
            )
            self.check_cfg: AnomalyDetectionMetricCheckCfg
            self.skip_anomaly_check = False
            metric_name = self.check_cfg.metric_name
            metric = self.metrics[metric_name]
            self.historic_descriptors[KEY_HISTORIC_MEASUREMENTS] = HistoricMeasurementsDescriptor(
                metric_identity=metric.identity,
                limit=HISTORIC_RESULTS_LIMIT,
            )
            self.historic_descriptors[KEY_HISTORIC_CHECK_RESULTS] = HistoricCheckResultsDescriptor(
                check_identity=self.create_identity(), limit=HISTORIC_RESULTS_LIMIT
            )
            self.diagnostics = {}
            self.cloud_check_type = "anomalyDetection"
        except Exception as e:
            self.skip_anomaly_check = True
            data_source_scan.scan._logs.error(
                f"""An error occurred during the initialization of AnomalyMetricCheck. Please make sure"""
                f""" that the metric '{check_cfg.metric_name}' is supported. For more information see"""
                f""" the docs: https://docs.soda.io/soda-cl/anomaly-detection.html""",
                exception=e,
            )

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, dict[str, Any]]) -> None:
        if self.skip_anomaly_check:
            return

        if not isinstance(historic_values, dict):
            self.logs.error(
                "Getting historical measurements and check results from Soda Cloud resulted in a "
                f"{type(historic_values)} object which is not compatible with anomaly detection. "
                "Check previous log messages for more information."
            )
            return

        historic_check_results = historic_values.get(KEY_HISTORIC_CHECK_RESULTS, {}).get("check_results", {})
        historic_measurements = self.get_historic_measurements(metrics, historic_values)

        # TODO test for module installation and set check status to is_skipped if the module is not installed
        try:
            from soda.scientific.anomaly_detection_v2.anomaly_detector import (
                AnomalyDetector,
            )
        except ModuleNotFoundError as e:
            self.logs.error(f"{SODA_SCIENTIFIC_MISSING_LOG_MESSAGE}\n Original error: {e}")
            return

        anomaly_detector = AnomalyDetector(
            measurements=historic_measurements,
            check_results=historic_check_results,
            logs=self.logs,
            model_cfg=self.check_cfg.model_cfg,
            training_dataset_params=self.check_cfg.training_dataset_params,
            severity_level_params=self.check_cfg.severity_level_params,
        )
        level, diagnostics = anomaly_detector.evaluate()
        assert isinstance(diagnostics, dict), f"Anomaly diagnostics should be a dict. Got a {type(diagnostics)} instead"

        self.add_outcome_reason(
            outcome_type=diagnostics["anomalyErrorCode"],
            message=diagnostics["anomalyErrorMessage"],
            severity=diagnostics["anomalyErrorSeverity"],
        )
        self.diagnostics = diagnostics

        if diagnostics["anomalyErrorCode"] == "not_enough_measurements_custom":
            if self.diagnostics["value"] is None:
                self.diagnostics["value"] = self.get_metric_value()
            return
        self.outcome = CheckOutcome(level)

    def get_historic_measurements(
        self, metrics: dict[str, Metric], historic_values: dict[str, dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        metric_name = self.check_cfg.metric_name
        historic_measurements = historic_values.get(KEY_HISTORIC_MEASUREMENTS, {}).get("measurements", {})
        self.logs.debug(
            "Anomaly Detection: using historical measurements " f"for identity {metrics[metric_name].identity}"
        )
        if not historic_measurements:
            self.logs.warning(f"This is the first time that we derive {metrics[metric_name]} metric")
            historic_measurements["results"] = []

        # Append current results
        local_data_time = self.data_source_scan.scan._data_timestamp
        utc_data_time = local_data_time.astimezone(timezone.utc)
        utc_data_time_str = utc_data_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        historic_measurements.get("results", []).append(
            {
                "id": "dummy_id",  # Placeholder number that will be overwritten
                "identity": metrics[metric_name].identity,
                "value": self.get_metric_value(),
                "dataTime": utc_data_time_str,
            }
        )
        return historic_measurements

    def get_cloud_diagnostics_dict(self) -> dict:
        cloud_diagnostics = super().get_cloud_diagnostics_dict()
        return {**cloud_diagnostics, **self.diagnostics}

    def get_log_diagnostic_dict(self) -> dict:
        log_diagnostics = super().get_log_diagnostic_dict()
        if self.historic_diff_values:
            log_diagnostics.update(self.diagnostics)
        return log_diagnostics

    def create_migrate_identities(self) -> dict[str, str] | None:
        """
        This method is used to migrate the identites from anomaly score to anomaly detection.
        It's a hack to obtain the same identity for the anomaly detection check as the anomaly score check.
        """
        if self.check_cfg.take_over_existing_anomaly_score_check is False:
            # Do not migrate identities if the flag is set to False
            return super().create_migrate_identities()
        original_source_line = self.check_cfg.source_line.strip()
        original_migrate_data_source_name = self.data_source_scan.data_source.migrate_data_source_name

        hacked_source_line = original_source_line.replace("anomaly detection", "anomaly score") + " < default"
        hacked_migrate_data_source_name = original_migrate_data_source_name
        if original_migrate_data_source_name is None:
            hacked_migrate_data_source_name = True

        self.check_cfg.source_line = hacked_source_line
        self.data_source_scan.data_source.migrate_data_source_name = hacked_migrate_data_source_name

        identities = super().create_migrate_identities()

        # Overwrite the original source line and migrate data source name to avoid confusion
        self.check_cfg.source_line = original_source_line
        self.data_source_scan.data_source.migrate_data_source_name = original_migrate_data_source_name
        return identities
