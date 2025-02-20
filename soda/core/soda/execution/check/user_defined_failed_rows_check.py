from typing import Dict

from soda.execution.check.check import Check
from soda.execution.check_outcome import CheckOutcome
from soda.execution.metric.metric import Metric
from soda.execution.metric.user_defined_failed_rows_metric import (
    UserDefinedFailedRowsMetric,
)

KEY_FAILED_ROWS_COUNT = "failed_rows_count"


class UserDefinedFailedRowsCheck(Check):
    """
    Eg:

    checks:
      - "Customers must have cst_size":
          failed rows query: |
            SELECT *
            FROM {table_name}
            WHERE cst_size < 0
    """

    def __init__(
        self, check_cfg: "UserDefinedFailedRowsCheckCfg", data_source_scan: "DataSourceScan", partition: "Partition"
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=None,
        )

        from soda.sodacl.user_defined_failed_rows_check_cfg import (
            UserDefinedFailedRowsCheckCfg,
        )

        check_cfg: UserDefinedFailedRowsCheckCfg = self.check_cfg
        self.check_value = None

        jinja_resolve = self.data_source_scan.scan.jinja_resolve

        metric = UserDefinedFailedRowsMetric(
            data_source_scan=self.data_source_scan,
            check_name=check_cfg.source_line,
            query=jinja_resolve(check_cfg.query, self.check_cfg.variables),
            check=self,
            partition=partition,
        )
        metric = self.data_source_scan.resolve_metric(metric)
        self.metrics[KEY_FAILED_ROWS_COUNT] = metric

    def evaluate(self, metrics: Dict[str, Metric], historic_values: Dict[str, object]):
        metric = metrics.get(KEY_FAILED_ROWS_COUNT)
        self.check_value: int = metrics.get(KEY_FAILED_ROWS_COUNT).value

        # Thresholds path
        if self.check_cfg.fail_threshold_cfg or self.check_cfg.warn_threshold_cfg:
            if self.check_cfg.fail_threshold_cfg and self.check_cfg.fail_threshold_cfg.is_bad(self.check_value):
                self.outcome = CheckOutcome.FAIL
            elif self.check_cfg.warn_threshold_cfg and self.check_cfg.warn_threshold_cfg.is_bad(self.check_value):
                self.outcome = CheckOutcome.WARN
            else:
                self.outcome = CheckOutcome.PASS
        else:
            # Original non-threshold path
            if self.check_value > 0:
                self.outcome = CheckOutcome.FAIL
            else:
                self.outcome = CheckOutcome.PASS

        self.failed_rows_sample_ref = metric.failed_rows_sample_ref
