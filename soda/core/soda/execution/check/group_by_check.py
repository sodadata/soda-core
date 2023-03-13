from __future__ import annotations

import copy

from soda.execution.check.check import Check
from soda.execution.check.metric_check import MetricCheck
from soda.execution.check_outcome import CheckOutcome
from soda.execution.metric.metric import Metric
from soda.execution.partition import Partition

GROUP_BY_RESULTS = "group_by_results"


class GroupByCheck(Check):
    def __init__(
        self,
        check_cfg: GroupByCheckCfg,
        data_source_scan: DataSourceScan,
        partition: Partition,
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=None,
        )
        from soda.sodacl.group_by_check_cfg import GroupByCheckCfg

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
        query_results = metrics[GROUP_BY_RESULTS].value
        group_limit = self.check_cfg.group_limit

        if len(query_results) > group_limit:
            raise Exception(
                f"Total number of groups {len(query_results)} exceeds configured group limit: {group_limit}"
            )

        fields = list(self.check_cfg.fields)
        group_check_cfgs = self.check_cfg.check_cfgs
        groups = [tuple(map(qr.get, fields)) for qr in query_results]

        group_checks = []
        for group in groups:
            for gcc in group_check_cfgs:
                config = copy.copy(gcc)
                config.name = gcc.name + f" [{','.join(str(v) for v in group)}]"
                column = ",".join(fields)
                gc = MetricCheck(config, self.data_source_scan, partition=self.partition, column=column)
                result = next(filter(lambda qr: tuple(map(qr.get, fields)) == group, query_results))
                if result is not None:
                    gc.check_value = result[config.metric_name]
                    metric = Metric(
                        self.data_source_scan,
                        self.partition,
                        column=None,
                        name=config.name,
                        check=None,
                        identity_parts=[],
                    )
                    metric.set_value(gc.check_value)
                    gc.metrics = {config.metric_name: metric}
                    gc.evaluate(metrics=None, historic_values=None)
                group_checks.append(gc)

        self.data_source_scan.scan._checks.extend(group_checks)

        # TODO decide what to do with global check state
        if all(gc.outcome == CheckOutcome.PASS for gc in group_checks):
            self.outcome = CheckOutcome.PASS
        elif any(gc.outcome == CheckOutcome.FAIL for gc in group_checks):
            self.outcome = CheckOutcome.FAIL
        else:
            self.outcome = CheckOutcome.PASS

    def get_cloud_diagnostics_dict(self) -> dict:
        group_by_diagnostics = {}
        return group_by_diagnostics

    def get_log_diagnostic_lines(self) -> list[str]:
        diagnostics_texts: list[str] = []
        return diagnostics_texts
