from abc import ABC
from typing import Dict, List, Optional

from soda.execution.check_outcome import CheckOutcome
from soda.execution.column import Column
from soda.execution.identity import Identity
from soda.execution.metric import Metric
from soda.soda_cloud.historic_descriptor import HistoricDescriptor
from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.distribution_check_cfg import DistributionCheckCfg


class Check(ABC):
    @staticmethod
    def create(
        check_cfg: "CheckCfg",
        partition: Optional["Partition"] = None,
        column: Optional["Column"] = None,
        data_source_scan: Optional["DataSourceScan"] = None,
    ) -> "Check":
        from soda.sodacl.anomaly_metric_check_cfg import AnomalyMetricCheckCfg
        from soda.sodacl.change_over_time_metric_check_cfg import (
            ChangeOverTimeMetricCheckCfg,
        )
        from soda.sodacl.freshness_check_cfg import FreshnessCheckCfg
        from soda.sodacl.metric_check_cfg import MetricCheckCfg
        from soda.sodacl.reference_check_cfg import ReferenceCheckCfg
        from soda.sodacl.row_count_comparison_check_cfg import (
            RowCountComparisonCheckCfg,
        )
        from soda.sodacl.schema_check_cfg import SchemaCheckCfg
        from soda.sodacl.user_defined_failed_rows_check_cfg import (
            UserDefinedFailedRowsCheckCfg,
        )
        from soda.sodacl.user_defined_failed_rows_expression_check_cfg import (
            UserDefinedFailedRowsExpressionCheckCfg,
        )

        if isinstance(check_cfg, ChangeOverTimeMetricCheckCfg):
            from soda.execution.change_over_time_metric_check import (
                ChangeOverTimeMetricCheck,
            )

            return ChangeOverTimeMetricCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, AnomalyMetricCheckCfg):
            from soda.execution.anomaly_metric_check import AnomalyMetricCheck

            return AnomalyMetricCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, MetricCheckCfg):
            from soda.execution.metric_check import MetricCheck

            return MetricCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, SchemaCheckCfg):
            from soda.execution.schema_check import SchemaCheck

            return SchemaCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, UserDefinedFailedRowsExpressionCheckCfg):
            from soda.execution.user_defined_failed_rows_expression_check import (
                UserDefinedFailedRowsExpressionCheck,
            )

            return UserDefinedFailedRowsExpressionCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, UserDefinedFailedRowsCheckCfg):
            from soda.execution.user_defined_failed_rows_check import (
                UserDefinedFailedRowsCheck,
            )

            return UserDefinedFailedRowsCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, RowCountComparisonCheckCfg):
            from soda.execution.row_count_comparison_check import (
                RowCountComparisonCheck,
            )

            return RowCountComparisonCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, ReferenceCheckCfg):
            from soda.execution.reference_check import ReferenceCheck

            return ReferenceCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, FreshnessCheckCfg):
            from soda.execution.freshness_check import FreshnessCheck

            return FreshnessCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, DistributionCheckCfg):
            from soda.execution.distribution_check import DistributionCheck

            return DistributionCheck(check_cfg, data_source_scan, partition, column)

        raise RuntimeError(f"Bug: Unsupported check type {type(check_cfg)}")

    def __init__(
        self,
        check_cfg: "CheckCfg",
        data_source_scan: "DataSourceScan",
        partition: Optional["Partition"],
        column: Optional["Column"],
        name: Optional[str],
        identity_parts: list,
    ):
        from soda.execution.partition import Partition

        self.name: str = name
        self.identity: str = Identity.create_identity(
            identity_type="check",
            data_source_scan=data_source_scan,
            partition=partition,
            column=column,
            name=name,
            identity_parts=identity_parts,
        )
        self.logs = data_source_scan.scan._logs
        self.check_cfg: CheckCfg = check_cfg
        self.data_source_scan = data_source_scan
        self.partition: Partition = partition
        self.column: Column = column
        self.metrics: Dict[str, Metric] = {}
        self.historic_descriptors: Dict[str, HistoricDescriptor] = {}
        self.cloud_check_type = "metricThreshold"

        # Check evaluation outcome
        self.outcome: CheckOutcome = None

    def create_definition(self):
        from soda.common.yaml_helper import to_yaml_str

        check_cfg = self.check_cfg
        if isinstance(check_cfg.source_configurations, dict):
            return to_yaml_str({check_cfg.source_header: [{check_cfg.source_line: check_cfg.source_configurations}]})
        else:
            return f"{check_cfg.source_header}:\n  {check_cfg.source_line}"

    def get_cloud_dict(self):
        from soda.execution.column import Column
        from soda.execution.partition import Partition

        return {
            "identity": self.identity,
            "name": self.generate_soda_cloud_check_name(),
            "type": self.cloud_check_type,
            "definition": self.create_definition(),
            "location": self.check_cfg.location.to_soda_cloud_json(),
            "dataSource": self.data_source_scan.data_source.data_source_name,
            "table": Partition.get_table_name(self.partition),
            # "filter": Partition.get_partition_name(self.partition), TODO: re-enable once backend supports the property.
            "column": Column.get_partition_name(self.column),
            "metrics": [metric.identity for metric in self.metrics.values()],
            "outcome": self.outcome.value,
            "diagnostics": self.get_cloud_diagnostics_dict(),
        }

    def generate_soda_cloud_check_name(self) -> str:
        if self.check_cfg.name:
            return self.check_cfg.name
        else:
            return self.check_cfg.source_line

    def get_cloud_diagnostics_dict(self) -> dict:
        return None

    def evaluate(self, metrics: Dict[str, Metric], historic_values: Dict[str, object]):
        raise NotImplementedError("Implement this abstract method")

    def get_log_diagnostic_lines(self) -> List[str]:
        log_diagnostic_lines = []

        log_diagnostic_dict = self.get_log_diagnostic_dict()
        if log_diagnostic_dict:
            for key, value in log_diagnostic_dict.items():
                log_diagnostic_lines.append(f"{key}: {value}")

        return log_diagnostic_lines

    def get_log_diagnostic_dict(self) -> dict:
        return self.get_cloud_diagnostics_dict()

    def get_summary(self) -> str:
        return self.check_cfg.name if self.check_cfg.name else self.check_cfg.source_line

    def __str__(self):
        log_diagnostic_lines = self.get_log_diagnostic_lines()
        if log_diagnostic_lines:
            diagnostics_text = ", ".join(log_diagnostic_lines)
            return f"[{self.check_cfg.source_line}] {self.outcome} ({diagnostics_text})"
        else:
            return f"[{self.check_cfg.source_line}] {self.outcome}"
