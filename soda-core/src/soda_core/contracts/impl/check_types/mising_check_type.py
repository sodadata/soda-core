from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import YamlObject
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, ContractYaml, CheckType


class MissingCheckType(CheckType):

    def __init__(self):
        super().__init__("missing")

    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        return MissingCheckYaml(
            check_yaml_object=check_yaml_object
        )


CheckType.register_check_type(MissingCheckType())


class MissingCheckYaml(CheckYaml):

    def __init__(
        self,
        check_yaml_object: YamlObject
    ):
        super().__init__(
            check_yaml_object=check_yaml_object
        )

    def create_check(
        self,
        check_yaml: CheckYaml,
        column_yaml: ColumnYaml | None,
        contract_yaml: ContractYaml,
        metrics_resolver: MetricsResolver,
        data_source: DataSource
    ) -> Check:
        return MissingCheck(
            contract_yaml=contract_yaml,
            column_yaml=column_yaml,
            check_yaml=self,
            metrics_resolver=metrics_resolver,
            data_source=data_source
        )


class MissingCheck(Check):

    def __init__(
        self,
        contract_yaml: ContractYaml,
        column_yaml: ColumnYaml | None,
        check_yaml: MissingCheckYaml,
        metrics_resolver: MetricsResolver,
        data_source: DataSource
    ):
        super().__init__(
            contract_yaml=contract_yaml,
            column_yaml=column_yaml,
            check_yaml=check_yaml,
        )

        missing_count_metric = MissingCountMetric(
            data_source_name=self.data_source_name,
            database_name=self.database_name,
            schema_name=self.schema_name,
            dataset_name=self.dataset_name,
            column_name=self.column_name
        )
        resolved_missing_count_metric: MissingCountMetric = metrics_resolver.resolve_metric(missing_count_metric)
        self.metrics.append(resolved_missing_count_metric)

        self.aggregation_metrics.append(resolved_missing_count_metric)

    def evaluate(self) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        return NumericIntegerMetricCheckResult(
            outcome=outcome,
            diagnostics={
                "missing_count": self.aggregation_metrics[0].measured_value
            }
        )


class MissingCountMetric(AggregationMetric):

    def __init__(
        self,
        data_source_name: str,
        database_name: str | None,
        schema_name: str | None,
        dataset_name: str,
        column_name: str
    ):
        super().__init__(
            data_source_name=data_source_name,
            database_name=database_name,
            schema_name=schema_name,
            dataset_name=dataset_name,
            column_name=column_name,
            metric_type_name="missing_count"
        )

    def sql_expression(self) -> SqlExpression:
        return COUNT(CASE_WHEN(IS_NULL(self.column_name), LITERAL(1), LITERAL(0)))

    def set_measured_value(self, value):
        self.measured_value = int(value)


class NumericIntegerMetricCheckResult(CheckResult):

    def __init__(self,
                 outcome: CheckOutcome,
                 diagnostics: dict[str, Number]
                 ):
        super().__init__(
            check_summary="Missing values count",
            outcome=outcome
        )
        outcome = outcome,
        self.diagnostics: dict[str, Number] = diagnostics

    def get_measurement(self, metric_name: str) -> Number:
        return self.diagnostics[metric_name]

    def get_contract_result_str_lines(self) -> list[str]:
        lines: list[str] = [
            f"Schema check {self.outcome.name}",
            f"  Actual: {self.diagnostics}",
        ]
        return lines
