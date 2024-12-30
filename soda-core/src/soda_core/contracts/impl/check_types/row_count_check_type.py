from __future__ import annotations

from time import thread_time

from soda_core.common.data_source import DataSource
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import YamlObject
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric, Threshold, \
    ThresholdType
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, ContractYaml, CheckType


class RowCountCheckType(CheckType):

    def get_check_type_names(self) -> list[str]:
        return ['row_count']

    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        return RowCountCheckYaml(
            check_yaml_object=check_yaml_object,
        )


class RowCountCheckYaml(CheckYaml):

    def __init__(
        self,
        check_yaml_object: YamlObject,
    ):
        super().__init__(
            check_yaml_object=check_yaml_object
        )
        self.parse_threshold(check_yaml_object=check_yaml_object)

    def create_check(self, data_source: DataSource, dataset_prefix: list[str] | None, contract_yaml: ContractYaml,
                     column_yaml: ColumnYaml | None, check_yaml: CheckYaml, metrics_resolver: MetricsResolver) -> Check:
        return RowCountCheck(
            data_source=data_source,
            dataset_prefix=dataset_prefix,
            contract_yaml=contract_yaml,
            check_yaml=self,
            metrics_resolver=metrics_resolver,
        )


class RowCountCheck(Check):

    def __init__(
        self,
        data_source: DataSource,
        dataset_prefix: list[str] | None,
        contract_yaml: ContractYaml,
        column_yaml: ColumnYaml | None,
        check_yaml: RowCountCheckYaml,
        metrics_resolver: MetricsResolver,
    ):
        threshold = Threshold.create(check_yaml=check_yaml, default_threshold=Threshold(
            type=ThresholdType.SINGLE_COMPARATOR,
            must_be_greater_than=0
        ))
        summary = (self.threshold.get_assertion_summary(metric_name="row_count")
                   if self.threshold else "row_count (invalid threshold)")
        super().__init__(
            dataset_prefix=dataset_prefix,
            contract_yaml=contract_yaml,
            column_yaml=column_yaml,
            check_yaml=check_yaml,
            threshold=threshold,
            summary=summary
        )

        row_count_metric = RowCountMetric(
            data_source_name=self.data_source_name,
            dataset_prefix=self.dataset_prefix,
            dataset_name=self.dataset_name,
        )
        resolved_row_count_metric: RowCountMetric = metrics_resolver.resolve_metric(row_count_metric)
        self.metrics["row_count"] = resolved_row_count_metric
        self.aggregation_metrics.append(resolved_row_count_metric)

    def evaluate(self) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED
        row_count: int = self.metrics["row_count"].measured_value

        return CheckResult(
            outcome=outcome,
            check_summary=self.summary,
            diagnostic_lines=[
                f"Actual row_count was {row_count}"
            ],
        )

class RowCountMetric(AggregationMetric):

    def __init__(
        self,
        data_source_name: str,
        dataset_prefix: list[str] | None,
        dataset_name: str,
    ):
        super().__init__(
            data_source_name=data_source_name,
            dataset_prefix=dataset_prefix,
            dataset_name=dataset_name,
            metric_type_name="row_count"
        )

    def sql_expression(self) -> SqlExpression:
        return COUNT(STAR())

    def set_measured_value(self, value):
        self.measured_value = int(value)
