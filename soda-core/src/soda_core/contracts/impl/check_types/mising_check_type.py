from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.common.yaml import YamlObject
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.check_types.row_count_check_type import RowCountMetric
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric, Threshold, \
    ThresholdType, DerivedPercentageMetric, CheckParser, Contract, Column
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, CheckYamlParser


class MissingCheckYamlParser(CheckYamlParser):

    def get_check_type_names(self) -> list[str]:
        return ['missing_count', 'missing_percent']

    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        return MissingCheckYaml(
            check_yaml_object=check_yaml_object,
        )


class MissingCheckYaml(CheckYaml):

    def __init__(
        self,
        check_yaml_object: YamlObject,
    ):
        super().__init__(
            check_yaml_object=check_yaml_object
        )
        self.parse_threshold(check_yaml_object)


class MissingCheckParser(CheckParser):

    def get_check_type_names(self) -> list[str]:
        return ['missing_count', 'missing_percent']

    def parse_check(
        self,
        contract: Contract,
        column: Column | None,
        check_yaml: MissingCheckYaml,
        metrics_resolver: MetricsResolver,
    ) -> Check | None:
        return MissingCheck(
            contract=contract,
            column=column,
            check_yaml=check_yaml,
            metrics_resolver=metrics_resolver,
        )


class MissingCheck(Check):

    def __init__(
        self,
        contract: Contract,
        column: Column,
        check_yaml: MissingCheckYaml,
        metrics_resolver: MetricsResolver,
    ):
        super().__init__(
            contract=contract,
            column=column,
            check_yaml=check_yaml,
        )
        self.threshold = Threshold.create(
            check_yaml=check_yaml,
            default_threshold=Threshold(type=ThresholdType.SINGLE_COMPARATOR,must_be=0)
        )
        self.summary = (
            self.threshold.get_assertion_summary(metric_name=check_yaml.type) if self.threshold
            else f"{check_yaml.type} (invalid threshold)"
        )

        missing_count_metric = MissingCountMetric(
            contract=contract,
            column=column,
            check=self
        )
        resolved_missing_count_metric: MissingCountMetric = metrics_resolver.resolve_metric(missing_count_metric)
        self.metrics["missing_count"] = resolved_missing_count_metric
        self.aggregation_metrics.append(resolved_missing_count_metric)

        if self.type == "missing_percent":
            row_count_metric = RowCountMetric(
                data_source_name=self.data_source_name,
                dataset_prefix=self.dataset_prefix,
                dataset_name=self.dataset_name
            )
            resolved_row_count_metric: RowCountMetric = metrics_resolver.resolve_metric(row_count_metric)
            self.metrics["row_count"] = resolved_row_count_metric
            self.aggregation_metrics.append(resolved_row_count_metric)

            self.metrics["missing_percent"] = DerivedPercentageMetric(
                metric_name="missing_percent",
                fraction_metric=resolved_missing_count_metric,
                total_metric=resolved_row_count_metric
            )

    def evaluate(self) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        missing_count: int = self.metrics["missing_count"].value
        diagnostic_lines = [
            f"Actual missing_count was {missing_count}"
        ]

        threshold_value: Number | None = None
        if self.type == "missing_count":
            threshold_value = missing_count
        else:
            row_count: int = self.metrics["row_count"].value
            diagnostic_lines.append(f"Actual row_count was {row_count}")
            if row_count > 0:
                missing_percent: float = self.metrics["missing_percent"].value
                diagnostic_lines.append(f"Actual missing_percent was {missing_percent}")
                threshold_value = missing_percent

        if self.threshold and isinstance(threshold_value, Number):
            if self.threshold.passes(threshold_value):
                outcome = CheckOutcome.PASSED
            else:
                outcome = CheckOutcome.FAILED

        return CheckResult(
            outcome=outcome,
            check_summary=self.summary,
            diagnostic_lines=diagnostic_lines,
        )


class MissingCountMetric(AggregationMetric):

    def __init__(
        self,
        contract: Contract,
        column: Column,
        check: Check,
    ):
        super().__init__(
            contract=contract,
            column=column,
            metric_type_name=check.type,
            qualifier=check.qualifier
        )

    def sql_expression(self) -> SqlExpression:
        is_missing_clauses: list[SqlExpression] = [IS_NULL(self.column_name)]
        if isinstance(self.missing_configurations.missing_values, list):
            literal_values = [LITERAL(value) for value in self.missing_configurations.missing_values]
            is_missing_clauses.append(IN(self.column_name, literal_values))
        ...TODO like regex...
        return SUM(CASE_WHEN(OR(is_missing_clauses), LITERAL(1), LITERAL(0)))

    def set_value(self, value):
        # expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if there are no rows
        value = 0 if value is None else value
        self.value = int(value)
