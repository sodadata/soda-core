from __future__ import annotations

from soda.execution.metric import Metric
from soda.execution.metric_check import MetricCheck
from soda.soda_cloud.historic_descriptor import HistoricDescriptor
from soda.sodacl.metric_check_cfg import MetricCheckCfg

KEY_HISTORIC_ANOMALY_VALUES = "historic_anomaly_values"


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

        self.historic_descriptors[KEY_HISTORIC_ANOMALY_VALUES] = HistoricDescriptor(
            metric=metric,
            anomaly_values=90,
        )

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        metric_value = self.get_metric_value()
        historic_anomaly_values = historic_values.get(KEY_HISTORIC_ANOMALY_VALUES)

        if historic_anomaly_values:
            self.check_value = self.compute_anomaly_score(historic_anomaly_values, metric_value)
            self.set_outcome_based_on_check_value()

            # put all diagnostics into a member field like this:
            self.anomaly_values = {}

        else:
            self.logs.info("Skipping metric check eval because there is not enough historic data yet")

    def compute_anomaly_score(self, historic_anomaly_values: list[dict], metric_value: int | float) -> float:
        data_timestamp = self.data_source_scan.scan._data_timestamp

        # TODO invoke the anomaly detection algorithm dynamically as it is in an extension module
        # ensure appropriate error log if the extension module is not installed
        # The historic_anomaly_values look like this:
        # [
        #     {
        #         'identity': 'metric-test_change_over_time.py::test_anomaly_detection-postgres-SODATEST_Customers_b7580920-row_count',
        #         'data_time': datetime.datetime(2022, 3, 8, 19, 40, 5, 880298),
        #         'value': 10
        #     },
        #     {
        #         'identity': 'metric-test_change_over_time.py::test_anomaly_detection-postgres-SODATEST_Customers_b7580920-row_count',
        #         'data_time': datetime.datetime(2022, 3, 7, 19, 40, 5, 880298),
        #         'value': 10
        #     }, ...
        # ]

        anomaly_score = 0.8

        return anomaly_score

    def get_cloud_diagnostics_dict(self) -> dict:
        cloud_diagnostics = super().get_cloud_diagnostics_dict()
        if self.historic_diff_values:
            cloud_diagnostics["anomaly_values"] = self.anomaly_values

    def get_log_diagnostic_dict(self) -> dict:
        log_diagnostics = super().get_log_diagnostic_dict()
        if self.historic_diff_values:
            log_diagnostics.update(self.anomaly_values)
        return log_diagnostics
