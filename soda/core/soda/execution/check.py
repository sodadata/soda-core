from __future__ import annotations

import collections
from abc import ABC, abstractmethod

from soda.execution.check_outcome import CheckOutcome
from soda.execution.column import Column
from soda.execution.identity import ConsistentHashBuilder
from soda.execution.metric import Metric
from soda.sampler.sample_ref import SampleRef
from soda.soda_cloud.historic_descriptor import HistoricDescriptor
from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.distribution_check_cfg import DistributionCheckCfg


class Check(ABC):
    @staticmethod
    def create(
        check_cfg: CheckCfg,
        partition: Partition | None = None,
        column: Column | None = None,
        data_source_scan: DataSourceScan | None = None,
    ) -> Check:
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
        check_cfg: CheckCfg,
        data_source_scan: DataSourceScan,
        partition: Partition | None,
        column: Column | None,
        name: str | None,
    ):
        from soda.execution.partition import Partition

        self.name: str = name
        self.check_cfg: CheckCfg = check_cfg
        self.logs = data_source_scan.scan._logs
        self.data_source_scan = data_source_scan
        self.partition: Partition = partition
        self.column: Column = column
        self.metrics: dict[str, Metric] = {}
        self.historic_descriptors: dict[str, HistoricDescriptor] = {}
        self.cloud_check_type = "metricThreshold"
        # in the evaluate method, checks can optionally extract a failed rows sample ref from the metric
        self.failed_rows_sample_ref: SampleRef | None = None

        # Attribute for automated monitoring
        self.archetype = None

        # Check evaluation outcome
        self.outcome: CheckOutcome | None = None

    def create_definition(self) -> str:
        check_cfg: CheckCfg = self.check_cfg
        from soda.common.yaml_helper import to_yaml_str

        if isinstance(check_cfg.source_configurations, dict):
            return to_yaml_str({check_cfg.source_header: [{check_cfg.source_line: check_cfg.source_configurations}]})
        else:
            return f"{check_cfg.source_header}:\n  {check_cfg.source_line}"

    def create_identity(self) -> str:
        check_cfg: CheckCfg = self.check_cfg
        from soda.common.yaml_helper import to_yaml_str

        if isinstance(check_cfg.source_configurations, dict):
            identity = check_cfg.source_configurations.get("identity")
            if isinstance(identity, str):
                return identity

        hash_builder = ConsistentHashBuilder()
        # Note: In case of for each table, the check_cfg.source_header will contain the actual table name as well
        hash_builder.add(check_cfg.source_header)
        hash_builder.add(check_cfg.source_line)

        # If the check is automated monitoring append "automated_monitoring" tag
        if hasattr(check_cfg, "is_automated_monitoring") and check_cfg.is_automated_monitoring:
            hash_builder.add("automated_monitoring")

        if isinstance(check_cfg.source_configurations, dict):

            identity_source_configurations = dict(check_cfg.source_configurations)
            # The next lines ensures that configuration properties 'name' and 'identity' are ignored
            # for computing the check identity
            identity_source_configurations.pop("name", None)
            if len(identity_source_configurations) > 0:
                # The next line ensures that ordering of the check configurations don't matter for identity
                identity_source_configurations = collections.OrderedDict(sorted(identity_source_configurations.items()))
                identity_source_configurations_yaml = to_yaml_str(identity_source_configurations)
                hash_builder.add(identity_source_configurations_yaml)
        return hash_builder.get_hash()

    @staticmethod
    def __check_source_to_yaml(source_header: str, source_line: str, source_configurations: dict | None) -> str:
        from soda.common.yaml_helper import to_yaml_str

        if not isinstance(source_configurations, dict):
            return f"{source_header}:\n  {source_line}"
        return to_yaml_str({source_header: [{source_line: source_configurations}]})

    def get_cloud_dict(self):
        from soda.execution.column import Column
        from soda.execution.partition import Partition

        cloud_dict = {
            "identity": self.create_identity(),
            "name": self.generate_soda_cloud_check_name(),
            "type": self.cloud_check_type,
            "definition": self.create_definition(),
            "location": self.check_cfg.location.to_soda_cloud_json(),
            "dataSource": self.data_source_scan.data_source.data_source_name,
            "table": Partition.get_table_name(self.partition),
            # "filter": Partition.get_partition_name(self.partition), TODO: re-enable once backend supports the property.
            "column": Column.get_partition_name(self.column),
            "metrics": [metric.identity for metric in self.metrics.values()],
            "outcome": self.outcome.value if self.outcome else None,
            "diagnostics": self.get_cloud_diagnostics_dict(),
        }
        # Update dict if automated monitoring is running
        if self.archetype is not None:
            cloud_dict.update({"archetype": self.archetype})

        return cloud_dict

    def generate_soda_cloud_check_name(self) -> str:
        if self.check_cfg.name:
            return self.check_cfg.name

        name = self.check_cfg.source_line

        if self.check_cfg.source_configurations:
            source_cfg = dict(self.check_cfg.source_configurations)

            if source_cfg.get("warn"):
                name += f" warn {source_cfg['warn']}"

            if source_cfg.get("fail"):
                name += f" fail {source_cfg['fail']}"

        return name

    @abstractmethod
    def get_cloud_diagnostics_dict(self) -> dict:
        pass

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        raise NotImplementedError("Implement this abstract method")

    def get_log_diagnostic_lines(self) -> list[str]:
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
