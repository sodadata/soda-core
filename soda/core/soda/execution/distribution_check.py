from numbers import Number
from typing import Dict, Optional

from soda.execution.check import Check
from soda.execution.check_outcome import CheckOutcome
from soda.execution.column import Column
from soda.execution.data_source_scan import DataSourceScan
from soda.execution.metric import Metric
from soda.execution.partition import Partition
from soda.execution.query import Query
from soda.sodacl.distribution_check_cfg import DistributionCheckCfg

from soda.scientific.distribution.comparison import DistributionChecker


class DistributionCheck(Check):
    def __init__(
        self,
        check_cfg: DistributionCheckCfg,
        data_source_scan: DataSourceScan,
        partition: Optional[Partition] = None,
        column: Optional[Column] = None,
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=column,
            name="distribution_difference",
            identity_parts=check_cfg.get_identity_parts(),
        )

    def evaluate(self, metrics: Dict[str, Metric], historic_values: Dict[str, object]):
        distribution_check_cfg: DistributionCheckCfg = self.check_cfg

        sql = self.sql_column_values_query(distribution_check_cfg)

        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name="get_values_for_distro_check",
            sql=sql,
        )
        query.execute()
        if query.exception is None and query.rows is not None:
            test_data = [row[0] for row in query.rows]

            _, p_value = DistributionChecker(distribution_check_cfg, test_data).run()
            self.check_value = p_value

            self.set_outcome_based_on_check_value()

    def set_outcome_based_on_check_value(self):
        from soda.sodacl.distribution_check_cfg import DistributionCheckCfg

        distribution_check_cfg: DistributionCheckCfg = self.check_cfg
        if self.check_value is not None and (
            distribution_check_cfg.warn_threshold_cfg or distribution_check_cfg.fail_threshold_cfg
        ):
            if isinstance(self.check_value, Number):
                if distribution_check_cfg.fail_threshold_cfg and distribution_check_cfg.fail_threshold_cfg.is_bad(
                    self.check_value
                ):
                    self.outcome = CheckOutcome.FAIL
                elif distribution_check_cfg.warn_threshold_cfg and distribution_check_cfg.warn_threshold_cfg.is_bad(
                    self.check_value
                ):
                    self.outcome = CheckOutcome.WARN
                else:
                    self.outcome = CheckOutcome.PASS
            else:
                self.logs.error(
                    f"Cannot evaluate check: Expected a numeric value, but was {self.check_value}",
                    location=self.check_cfg.location,
                )

    def get_cloud_diagnostics_dict(self) -> dict:
        cloud_diagnostics = super().get_cloud_diagnostics_dict()
        # if self.historic_diff_values:
        #     cloud_diagnostics["historic_diff_values"] = self.historic_diff_values
        return cloud_diagnostics

    def get_log_diagnostic_dict(self) -> dict:
        log_diagnostics = super().get_log_diagnostic_dict()
        # if self.historic_diff_values:
        #     log_diagnostics.update(self.historic_diff_values)
        return log_diagnostics

    def sql_column_values_query(self, distribution_check_cfg):
        column_name = distribution_check_cfg.column_name
        sql = f"SELECT \n" f"  {column_name} \n" f"FROM {self.partition.table.prefixed_table_name}"

        partition_filter = self.partition.sql_partition_filter
        if partition_filter:
            scan = self.data_source_scan.scan
            resolved_filter = scan._jinja_resolve(definition=partition_filter)
            sql += f"\nWHERE {resolved_filter}"

        return sql
