from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.check_types.row_count_check_yaml import RowCountCheckYaml
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric, Threshold, \
    ThresholdType, CheckParser, Contract, Column


class RowCountCheckParser(CheckParser):

    def get_check_type_names(self) -> list[str]:
        return ['row_count']

    def parse_check(
        self,
        contract: Contract,
        column: Column | None,
        check_yaml: RowCountCheckYaml,
        metrics_resolver: MetricsResolver,
    ) -> Check | None:
        return RowCountCheck(
            contract=contract,
            column=column,
            check_yaml=check_yaml,
            metrics_resolver=metrics_resolver,
        )


class RowCountCheck(Check):

    def __init__(
        self,
        contract: Contract,
        column: Column | None,
        check_yaml: RowCountCheckYaml,
        metrics_resolver: MetricsResolver,
    ):
        super().__init__(
            contract=contract,
            column=column,
            check_yaml=check_yaml,
        )

        self.threshold = Threshold.create(
            check_yaml=check_yaml,
            default_threshold=Threshold(
                type=ThresholdType.SINGLE_COMPARATOR,
                must_be_greater_than=0
            )
        )

        self.summary = (
            self.threshold.get_assertion_summary(metric_name="row_count") if self.threshold
            else "row_count (invalid threshold)"
        )

        row_count_metric = RowCountMetric(
            contract=contract,
        )
        resolved_row_count_metric: RowCountMetric = metrics_resolver.resolve_metric(row_count_metric)
        self.metrics["row_count"] = resolved_row_count_metric

    def evaluate(self) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED
        row_count: int = self.metrics["row_count"].value

        if self.threshold:
            if self.threshold.passes(row_count):
                outcome = CheckOutcome.PASSED
            else:
                outcome = CheckOutcome.FAILED

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
        contract: Contract,
    ):
        super().__init__(
            contract=contract,
            metric_type="row_count",
        )

    def sql_expression(self) -> SqlExpression:
        return COUNT(STAR())

    def set_value(self, value: any) -> None:
        self.value = int(value)
