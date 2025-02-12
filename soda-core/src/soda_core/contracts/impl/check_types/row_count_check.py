from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome, CheckInfo, ContractInfo
from soda_core.contracts.impl.check_types.row_count_check_yaml import RowCountCheckYaml
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric, Threshold, \
    ThresholdType, CheckParser, Contract, Column, MeasurementValues


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

        metric_name: str = Threshold.get_metric_name(check_yaml.type, column=column)

        self.name = check_yaml.name if check_yaml.name else (
            self.threshold.get_assertion_summary(metric_name=metric_name) if self.threshold
            else f"{check_yaml.type} (invalid threshold)"
        )

        self.summary = (
            self.threshold.get_assertion_summary(metric_name=metric_name) if self.threshold
            else f"{check_yaml.type} (invalid threshold)"
        )

        self.row_count_metric = self._resolve_metric(RowCountMetric(
            contract=contract,
        ))

    def evaluate(self, measurement_values: MeasurementValues, contract_info: ContractInfo) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED
        row_count: int = measurement_values.get_value(self.row_count_metric)

        if self.threshold:
            if self.threshold.passes(row_count):
                outcome = CheckOutcome.PASSED
            else:
                outcome = CheckOutcome.FAILED

        diagnostic_lines: list[str] = [
            f"Actual row_count was {row_count}"
        ]

        return CheckResult(
            contract=contract_info,
            check=self._build_check_info(),
            metric_value=row_count,
            outcome=outcome,
            diagnostic_lines=diagnostic_lines,
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

    def convert_db_value(self, value: any) -> any:
        return int(value)
