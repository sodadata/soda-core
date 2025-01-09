from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.check_types.missing_check_yaml import MissingCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetric
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric, Threshold, \
    ThresholdType, DerivedPercentageMetric, CheckParser, Contract, Column, MissingAndValidity, MissingAndValidityCheck
from soda_core.contracts.impl.contract_yaml import ColumnYaml, CheckYaml


class InvalidCheckParser(CheckParser):

    def get_check_type_names(self) -> list[str]:
        return ['invalid_count', 'invalid_percent']

    def parse_check(
        self,
        contract: Contract,
        column: Column | None,
        check_yaml: MissingCheckYaml,
        metrics_resolver: MetricsResolver,
    ) -> Check | None:
        return InvalidCheck(
            contract=contract,
            column=column,
            check_yaml=check_yaml,
            metrics_resolver=metrics_resolver,
        )


class InvalidCheck(MissingAndValidityCheck):

    def __init__(
        self,
        contract: Contract,
        column: Column,
        check_yaml: InvalidCheckYaml,
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

        invalid_count_metric = InvalidCountMetric(
            contract=contract,
            column=column,
            check=self
        )
        resolved_invalid_count_metric: InvalidCountMetric = metrics_resolver.resolve_metric(invalid_count_metric)
        self.metrics["invalid_count"] = resolved_invalid_count_metric

        if self.type == "invalid_percent":
            row_count_metric = RowCountMetric(
                contract=contract,
            )
            resolved_row_count_metric: RowCountMetric = metrics_resolver.resolve_metric(row_count_metric)
            self.metrics["row_count"] = resolved_row_count_metric

            self.metrics["invalid_percent"] = metrics_resolver.resolve_metric(DerivedPercentageMetric(
                metric_type="invalid_percent",
                fraction_metric=resolved_invalid_count_metric,
                total_metric=resolved_row_count_metric
            ))

    def evaluate(self) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        invalid_count: int = self.metrics["invalid_count"].value
        diagnostic_lines = [
            f"Actual invalid_count was {invalid_count}"
        ]

        threshold_value: Number | None = None
        if self.type == "invalid_count":
            threshold_value = invalid_count
        else:
            row_count: int = self.metrics["row_count"].value
            diagnostic_lines.append(f"Actual row_count was {row_count}")
            if row_count > 0:
                missing_percent: float = self.metrics["invalid_percent"].value
                diagnostic_lines.append(f"Actual invalid_percent was {missing_percent}")
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


class InvalidCountMetric(AggregationMetric):

    def __init__(
        self,
        contract: Contract,
        column: Column,
        check: MissingAndValidityCheck,
    ):
        super().__init__(
            contract=contract,
            column=column,
            metric_type=check.type,
        )
        self.missing_and_validity: MissingAndValidity = check.missing_and_validity

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column.column_yaml.name
        return self.missing_and_validity.get_sum_invalid_count_expr(column_name)

    def set_value(self, value):
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        value = 0 if value is None else value
        self.value = int(value)
