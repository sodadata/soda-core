from typing import Dict, Optional

from soda.execution.metric import Metric
from soda.execution.metric_check import MetricCheck
from soda.soda_cloud.historic_descriptor import HistoricDescriptor
from soda.sodacl.metric_check_cfg import MetricCheckCfg

KEY_HISTORIC_METRIC_AGGREGATE = "historic_metric_aggregate"


class ChangeOverTimeMetricCheck(MetricCheck):
    def __init__(
        self,
        check_cfg: "MetricCheckCfg",
        data_source_scan: "DataSourceScan",
        partition: Optional["Partition"] = None,
        column: Optional["Column"] = None,
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

        self.historic_descriptors[KEY_HISTORIC_METRIC_AGGREGATE] = HistoricDescriptor(
            metric=metric,
            change_over_time_cfg=metric_check_cfg.change_over_time_cfg,
        )

    def evaluate(self, metrics: Dict[str, Metric], historic_values: Dict[str, object]):

        metric_value = self.get_metric_value()

        historic_value = historic_values.get(KEY_HISTORIC_METRIC_AGGREGATE)
        if historic_value is not None:
            if isinstance(metric_value, int) and isinstance(historic_value, int):
                self.check_value = metric_value - historic_value
            else:
                self.check_value = float(metric_value) - float(historic_value)

            self.historic_diff_values = {
                "historic_value": historic_value,
                "metric_value": metric_value,
            }

            self.set_outcome_based_on_check_value()

        else:
            self.logs.info("Skipping metric check eval because there is not enough historic data yet")

    def get_cloud_diagnostics_dict(self) -> dict:
        cloud_diagnostics = super().get_cloud_diagnostics_dict()
        if self.historic_diff_values:
            cloud_diagnostics["historic_diff_values"] = self.historic_diff_values

    def get_log_diagnostic_dict(self) -> dict:
        log_diagnostics = super().get_log_diagnostic_dict()
        if self.historic_diff_values:
            log_diagnostics.update(self.historic_diff_values)
        return log_diagnostics
