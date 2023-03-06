from typing import List

from soda.execution.check.check import Check
from soda.execution.metric.metric import Metric
from soda.execution.partition import Partition

GROUP_BY_RESULTS = 'group_by_results'


class GroupByCheck(Check):
    def __init__(
        self,
        check_cfg: "GroupByCheckCfg",
        data_source_scan: "DataSourceScan",
        partition: Partition,
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=None,
        )
        from soda.sodacl.group_by_cfg import GroupByCheckCfg
        check_cfg: GroupByCheckCfg = self.check_cfg
        self.check_value = None
        from soda.execution.metric.group_by_metric import GroupByMetric

        group_by_metric = data_source_scan.resolve_metric(
            GroupByMetric(
                data_source_scan=self.data_source_scan,
                partition=partition,
                query=check_cfg.query,
                check=self,

            )
        )
        self.metrics[GROUP_BY_RESULTS] = group_by_metric

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        raise NotImplementedError()

    def get_cloud_diagnostics_dict(self) -> dict:
        group_by_diagnostics = {}
        return group_by_diagnostics

    def get_log_diagnostic_lines(self) -> List[str]:
        diagnostics_texts: List[str] = []
        return diagnostics_texts
