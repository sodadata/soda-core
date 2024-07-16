from __future__ import annotations

import collections
import os
from abc import ABC

from soda.cloud.cloud import Cloud
from soda.cloud.historic_descriptor import HistoricDescriptor
from soda.common.attributes_handler import AttributeHandler
from soda.common.string_helper import strip_quotes
from soda.execution.check_outcome import CheckOutcome
from soda.execution.check_type import CheckType
from soda.execution.column import Column
from soda.execution.identity import ConsistentHashBuilder
from soda.execution.metric.metric import Metric
from soda.execution.query.query import Query
from soda.sampler.sample_ref import SampleRef
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    AnomalyDetectionMetricCheckCfg,
)
from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.distribution_check_cfg import DistributionCheckCfg
from soda.sodacl.group_by_check_cfg import GroupByCheckCfg
from soda.sodacl.group_evolution_check_cfg import GroupEvolutionCheckCfg


class Check(ABC):
    @staticmethod
    def create(
        check_cfg: CheckCfg,
        partition: Partition | None = None,
        column: Column | None = None,
        data_source_scan: DataSourceScan | None = None,
    ) -> Check | None:
        from soda.sodacl.anomaly_detection_metric_check_cfg import (
            AnomalyDetectionMetricCheckCfg,
        )
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
            from soda.execution.check.change_over_time_metric_check import (
                ChangeOverTimeMetricCheck,
            )

            return ChangeOverTimeMetricCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, AnomalyMetricCheckCfg):
            from soda.execution.check.anomaly_metric_check import AnomalyMetricCheck

            return AnomalyMetricCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, AnomalyDetectionMetricCheckCfg):
            from soda.execution.check.anomaly_detection_metric_check import (
                AnomalyDetectionMetricCheck,
            )

            return AnomalyDetectionMetricCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, MetricCheckCfg):
            from soda.execution.check.metric_check import MetricCheck

            return MetricCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, SchemaCheckCfg):
            from soda.execution.check.schema_check import SchemaCheck

            return SchemaCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, UserDefinedFailedRowsExpressionCheckCfg):
            from soda.execution.check.user_defined_failed_rows_expression_check import (
                UserDefinedFailedRowsExpressionCheck,
            )

            return UserDefinedFailedRowsExpressionCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, UserDefinedFailedRowsCheckCfg):
            from soda.execution.check.user_defined_failed_rows_check import (
                UserDefinedFailedRowsCheck,
            )

            return UserDefinedFailedRowsCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, RowCountComparisonCheckCfg):
            from soda.execution.check.row_count_comparison_check import (
                RowCountComparisonCheck,
            )

            return RowCountComparisonCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, ReferenceCheckCfg):
            from soda.execution.check.reference_check import ReferenceCheck

            return ReferenceCheck(check_cfg, data_source_scan, partition)
        elif isinstance(check_cfg, FreshnessCheckCfg):
            from soda.execution.check.freshness_check import FreshnessCheck

            return FreshnessCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, DistributionCheckCfg):
            from soda.execution.check.distribution_check import DistributionCheck

            return DistributionCheck(check_cfg, data_source_scan, partition, column)

        elif isinstance(check_cfg, GroupByCheckCfg):
            from soda.execution.check.group_by_check import GroupByCheck

            return GroupByCheck(check_cfg, data_source_scan, partition)

        elif isinstance(check_cfg, GroupEvolutionCheckCfg):
            from soda.execution.check.group_evolution_check import GroupEvolutionCheck

            return GroupEvolutionCheck(check_cfg, data_source_scan, partition)

        raise RuntimeError(f"Bug: Unsupported check type {type(check_cfg)}")

    def __init__(
        self,
        check_cfg: CheckCfg,
        data_source_scan: DataSourceScan,
        partition: Partition | None,
        column: Column | None,
    ):
        from soda.execution.partition import Partition

        self.check_cfg: CheckCfg = check_cfg
        self.logs = data_source_scan.scan._logs
        self.data_source_scan: DataSourceScan = data_source_scan
        self.partition: Partition = partition
        self.column: Column = column
        self.metrics: dict[str, Metric] = {}
        self.attributes = {}
        self.historic_descriptors: dict[str, HistoricDescriptor] = {}
        self.cloud_check_type = "generic"
        # in the evaluate method, checks can optionally extract a failed rows sample ref from the metric
        self.failed_rows_sample_ref: SampleRef | None = None

        # Attribute for automated monitoring
        self.archetype = None

        # Check evaluation outcome
        self.outcome: CheckOutcome | None = None

        # Check outcome reasons in case of fail or pass
        self.outcome_reasons: list[dict] = []
        self.force_send_results_to_cloud = False

        # Default check type is Cloud, when this is set to CheckType.LOCAL, the check will not be sent to cloud
        self.check_type = CheckType.CLOUD

        self.cloud_dict = {}
        self.dict = {}

    @property
    def name(self) -> str:
        """User readable name.

        Uses user provided name if available or generates one from the check definition and thresholds.
        """
        jinja_resolve = self.data_source_scan.scan.jinja_resolve
        if self.check_cfg.name:
            return jinja_resolve(self.check_cfg.name)

        name = self.check_cfg.source_line

        if self.check_cfg.source_configurations:
            source_cfg = dict(self.check_cfg.source_configurations)

            if source_cfg.get("warn"):
                name += f" warn {source_cfg['warn']}"

            if source_cfg.get("fail"):
                name += f" fail {source_cfg['fail']}"

        return jinja_resolve(name)

    @property
    def is_deprecated(self) -> bool:
        return False

    def create_definition(self) -> str:
        check_cfg: CheckCfg = self.check_cfg
        from soda.common.yaml_helper import to_yaml_str

        if isinstance(check_cfg.source_configurations, dict):
            return to_yaml_str({check_cfg.source_header: [{check_cfg.source_line: check_cfg.source_configurations}]})
        else:
            return f"{check_cfg.source_header}:\n  {check_cfg.source_line}"

    def create_identity(self, with_datasource: bool | str = False, with_filename: bool = False) -> str:
        check_cfg: CheckCfg = self.check_cfg
        from soda.common.yaml_helper import to_yaml_str

        hash_builder = ConsistentHashBuilder()
        # Note: In case of for each table, the check_cfg.source_header will contain the actual table name as well
        hash_builder.add(check_cfg.source_header)
        hash_builder.add(check_cfg.source_line)

        # If the check is automated monitoring append "automated_monitoring" tag
        if hasattr(check_cfg, "is_automated_monitoring") and check_cfg.is_automated_monitoring:
            hash_builder.add("automated_monitoring")

        if isinstance(check_cfg.source_configurations, dict):
            identity_source_configurations = dict(check_cfg.source_configurations)
            # The next lines ensures that some of the configuration properties are ignored for computing the check identity
            identity_source_configurations.pop("name", None)
            identity_source_configurations.pop("samples limit", None)
            identity_source_configurations.pop("identity", None)
            identity_source_configurations.pop("attributes", None)
            identity_source_configurations.pop("template", None)
            identity_source_configurations.pop("warn_only", None)

            # Exlude hyperparameters / tuning configurations from identity for anomaly detection checks
            if isinstance(check_cfg, AnomalyDetectionMetricCheckCfg):
                identity_source_configurations.pop("take_over_existing_anomaly_score_check", None)
                identity_source_configurations.pop("training_dataset_parameters", None)
                identity_source_configurations.pop("model", None)
                identity_source_configurations.pop("severity_level_parameters", None)

            if len(identity_source_configurations) > 0:
                # The next line ensures that ordering of the check configurations don't matter for identity
                identity_source_configurations = collections.OrderedDict(sorted(identity_source_configurations.items()))
                identity_source_configurations_yaml = to_yaml_str(identity_source_configurations)
                hash_builder.add(identity_source_configurations_yaml)
        # Temp solution to introduce new variant of identity to help cloud identifying datasets with same name
        # See https://sodadata.atlassian.net/browse/CLOUD-1143
        if with_datasource:
            # Temp workaround to provide migration identities with fixed data source
            # name. See https://sodadata.atlassian.net/browse/CLOUD-5446
            for identity in self.identity_datasource_part() if isinstance(with_datasource, bool) else [with_datasource]:
                hash_builder.add(identity)

        if with_filename:
            hash_builder.add(os.path.basename(self.check_cfg.location.file_path))

        return hash_builder.get_hash()

    # Migrate Identities are created specifically to resolve https://sodadata.atlassian.net/browse/CLOUD-5447?focusedCommentId=30022
    # and can eventually be removed when all checks are migrated.
    def create_migrate_identities(self):
        migrate_data_source_name = self.data_source_scan.data_source.migrate_data_source_name
        if (
            migrate_data_source_name is None
            or self.data_source_scan.data_source.data_source_name == migrate_data_source_name
        ):
            return None

        identities = {
            "v1": self.create_identity(with_datasource=False, with_filename=False),
            "v2": self.create_identity(with_datasource=migrate_data_source_name, with_filename=False),
            "v3": self.create_identity(with_datasource=migrate_data_source_name, with_filename=True),
        }
        if isinstance(self.check_cfg.source_configurations, dict):
            identity = self.check_cfg.source_configurations.get("identity")
            if isinstance(identity, str):
                # append custom identity latest
                identities[f"v{len(identities) + 1}"] = identity
        return identities

    def identity_datasource_part(self) -> list[str]:
        return [
            self.data_source_scan.data_source.data_source_name,
        ]

    def add_outcome_reason(self, outcome_type: str, message: str, severity: str):
        self.force_send_results_to_cloud = True
        self.outcome_reasons.append({"code": outcome_type, "message": message, "severity": severity})  # error/warn/info

    @staticmethod
    def __check_source_to_yaml(source_header: str, source_line: str, source_configurations: dict | None) -> str:
        from soda.common.yaml_helper import to_yaml_str

        if not isinstance(source_configurations, dict):
            return f"{source_header}:\n  {source_line}"
        return to_yaml_str({source_header: [{source_line: source_configurations}]})

    def create_identities(self):
        identities = {
            "v1": self.create_identity(with_datasource=False, with_filename=False),
            "v2": self.create_identity(with_datasource=True, with_filename=False),
            "v3": self.create_identity(with_datasource=True, with_filename=True),
            # v4 is reserved for custom identity
        }
        if isinstance(self.check_cfg.source_configurations, dict):
            identity = self.check_cfg.source_configurations.get("identity")
            if isinstance(identity, str):
                identities["v4"] = identity
        return identities

    # Migrate Identities are created specifically to resolve https://sodadata.atlassian.net/browse/CLOUD-5447?focusedCommentId=30022
    # and can eventually be removed when all checks are migrated.
    def create_migrate_identities(self) -> dict[str, str] | None:
        migrate_data_source_name = self.data_source_scan.data_source.migrate_data_source_name
        if (
            migrate_data_source_name is None
            or self.data_source_scan.data_source.data_source_name == migrate_data_source_name
        ):
            return None

        identities = {
            "v1": self.create_identity(with_datasource=False, with_filename=False),
            "v2": self.create_identity(with_datasource=migrate_data_source_name, with_filename=False),
            "v3": self.create_identity(with_datasource=migrate_data_source_name, with_filename=True),
            # v4 is reserved for custom identity
        }
        if isinstance(self.check_cfg.source_configurations, dict):
            identity = self.check_cfg.source_configurations.get("identity")
            if isinstance(identity, str):
                identities["v4"] = identity
        return identities

    def get_cloud_dict(self):
        from soda.execution.column import Column
        from soda.execution.partition import Partition

        self.cloud_dict.update(
            {
                # See https://sodadata.atlassian.net/browse/CLOUD-1143
                "identity": self.create_identity(with_datasource=True, with_filename=True),
                "identities": self.create_identities(),
                "migratedIdentities": self.create_migrate_identities(),
                "name": self.name,
                "type": self.cloud_check_type,
                "definition": self.create_definition(),
                "resourceAttributes": self._format_attributes(),
                "location": self.check_cfg.location.get_cloud_dict(),
                "dataSource": self.data_source_scan.data_source.data_source_name,
                "table": strip_quotes(Partition.get_table_name(self.partition)),
                # "filter": Partition.get_partition_name(self.partition), TODO: re-enable once backend supports the property.
                "column": Column.get_partition_name(self.column),
                "metrics": [metric.identity for metric in self.metrics.values()],
                "outcome": self.outcome.value if self.outcome else None,
                "diagnostics": self.get_cloud_diagnostics_dict(),
                "source": "soda-core",
            }
        )
        # Update dict if automated monitoring is running
        if self.archetype is not None:
            self.cloud_dict.update({"archetype": self.archetype})

        # Update dict if check is skipped and we want to push reason to cloud
        if self.outcome_reasons:
            self.cloud_dict.update({"outcomeReasons": self.outcome_reasons})
        return self.cloud_dict

    def get_dict(self):
        from soda.execution.column import Column
        from soda.execution.partition import Partition

        self.dict.update(
            {
                "identity": self.create_identity(with_datasource=True, with_filename=True),
                "name": self.name,
                "type": self.cloud_check_type,
                "definition": self.create_definition(),
                "resourceAttributes": self._format_attributes(),
                "location": self.check_cfg.location.get_dict(),
                "dataSource": self.data_source_scan.data_source.data_source_name,
                "table": strip_quotes(Partition.get_table_name(self.partition)),
                "filter": Partition.get_partition_name(self.partition),
                "column": Column.get_partition_name(self.column),
                "metrics": [metric.identity for metric in self.metrics.values()],
                "outcome": self.outcome.value if self.outcome else None,
                "outcomeReasons": self.outcome_reasons,
                "archetype": self.archetype,
                "diagnostics": self.get_cloud_diagnostics_dict(),
            }
        )

        # "contract check id" is a property used by contracts implementation.
        # Here we propagate it from the check source configuration to the check result so that
        # the contracts implementation can correlate the sodacl check result with the contract check
        if self.check_cfg.source_configurations and "identity" in self.check_cfg.source_configurations:
            self.dict["source_identity"] = self.check_cfg.source_configurations.get("identity")

        return self.dict

    def get_cloud_diagnostics_dict(self) -> dict:
        csv_max_length = Cloud.CSV_TEXT_MAX_LENGTH

        cloud_diagnostics = {
            "blocks": [],
            "value": self.check_value if hasattr(self, "check_value") else None,
        }

        if self.failed_rows_sample_ref:
            if self.cloud_check_type == "generic":
                queries = self._get_all_related_queries()
                has_analysis_block = False
                sample_ref_block = self.failed_rows_sample_ref.get_cloud_diagnostics_block()

                for query in queries:
                    if query.failing_sql and query.passing_sql and sample_ref_block:
                        has_analysis_block = True
                        total_failing_rows = self.check_value

                        # TODO: temporary workaround until derived checks are refactored.
                        derived_failing_rows_formula_values = ["invalid_count", "duplicate_count", "missing_count"]
                        if hasattr(self, "formula_values") and self.formula_values:
                            for derived_value in derived_failing_rows_formula_values:
                                if derived_value in self.formula_values:
                                    total_failing_rows = self.formula_values[derived_value]

                        rca_block = {
                            "type": "failedRowsAnalysis",
                            "title": "Failed Rows Analysis",
                            "file": sample_ref_block["file"],
                            "failingRowsQueryName": f"{query.query_name}.failing_sql",
                            "passingRowsQueryName": f"{query.query_name}.passing_sql",
                            "totalFailingRows": total_failing_rows,
                            "sampleRowCount": sample_ref_block["file"]["storedRowCount"],
                        }
                        cloud_diagnostics["blocks"].append(rca_block)

                    # TODO: This should be a second failed rows file, refactor failed rows to support multiple files.
                    if (
                        query.failing_rows_sql_aggregated
                        and hasattr(query, "aggregated_failed_rows_data")
                        and query.aggregated_failed_rows_data
                    ):
                        text = f'{",".join(query.metric.metric_args)},frequency'

                        for row in query.aggregated_failed_rows_data:
                            row_str = f'\n{",".join(map(str, row))}'
                            if len(text) + len(row_str) < csv_max_length:
                                text += row_str

                        aggregate_rows_block = {
                            "type": "csv",
                            "title": "Failed Rows Aggregate",
                            "text": text,
                        }
                        cloud_diagnostics["blocks"].append(aggregate_rows_block)

                if not has_analysis_block:
                    cloud_diagnostics["blocks"].append(self.failed_rows_sample_ref.get_cloud_diagnostics_block())

            else:
                cloud_diagnostics["blocks"].append(self.failed_rows_sample_ref.get_cloud_diagnostics_block())

        return cloud_diagnostics

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
        diagnostics_dict = self.get_cloud_diagnostics_dict()
        diagnostics_dict.pop("failedRowsFile", None)
        diagnostics_dict.pop("blocks", None)
        return diagnostics_dict

    def __str__(self):
        log_diagnostic_lines = self.get_log_diagnostic_lines()
        if log_diagnostic_lines:
            diagnostics_text = ", ".join(log_diagnostic_lines)
            return f"[{self.check_cfg.source_line}] {self.outcome} ({diagnostics_text})"
        else:
            return f"[{self.check_cfg.source_line}] {self.outcome}"

    def _format_attributes(
        self,
    ) -> list[dict[str, any]]:
        attribute_handler = AttributeHandler(self.logs)

        attributes = []
        for k, v in self.attributes.items():
            attributes.append(
                {
                    "name": k,
                    "value": attribute_handler.format_attribute(v),
                }
            )

        return attributes

    def _get_all_related_queries(self) -> list(Query):
        queries = []

        for metric in self.metrics.values():
            for query in metric.queries:
                queries.append(query)

        return queries
