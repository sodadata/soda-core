from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome, ContractInfo
from soda_core.contracts.impl.check_types.missing_check_yaml import MissingCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetric
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric, Threshold, \
    ThresholdType, DerivedPercentageMetric, CheckParser, Contract, Column, MissingAndValidity, MissingAndValidityCheck, \
    Metric, MeasurementValues


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


class MissingCheck(MissingAndValidityCheck):

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

        # TODO create better support in class hierarchy for common vs specific stuff.  name is common.  see other check type impls
        metric_name: str = Threshold.get_metric_name(check_yaml.type, column=column)
        self.name = check_yaml.name if check_yaml.name else (
            self.threshold.get_assertion_summary(metric_name=metric_name) if self.threshold
            else f"{check_yaml.type} (invalid threshold)"
        )

        self.missing_count_metric = self._resolve_metric(MissingCountMetric(
            contract=contract,
            column=column,
            check=self
        ))

        if self.type == "missing_percent":
            self.row_count_metric: Metric = self._resolve_metric(RowCountMetric(
                contract=contract,
            ))

            self.missing_percent_metric: Metric = metrics_resolver.resolve_metric(DerivedPercentageMetric(
                metric_type="missing_percent",
                fraction_metric=self.missing_count_metric,
                total_metric=self.row_count_metric
            ))

    def evaluate(self, measurement_values: MeasurementValues, contract_info: ContractInfo) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        missing_count: int = measurement_values.get_value(self.missing_count_metric)
        diagnostic_lines = [
            f"Actual missing_count was {missing_count}"
        ]

        threshold_value: Number | None = None
        if self.type == "missing_count":
            threshold_value = missing_count
        else:
            row_count: int = measurement_values.get_value(self.row_count_metric)
            diagnostic_lines.append(f"Actual row_count was {row_count}")
            missing_percent: float = measurement_values.get_value(self.missing_percent_metric)
            diagnostic_lines.append(f"Actual missing_percent was {missing_percent}")
            threshold_value = missing_percent

        if self.threshold and isinstance(threshold_value, Number):
            if self.threshold.passes(threshold_value):
                outcome = CheckOutcome.PASSED
            else:
                outcome = CheckOutcome.FAILED

        return CheckResult(
            contract=contract_info,
            check=self._build_check_info(),
            metric_value=threshold_value,
            outcome=outcome,
            diagnostic_lines=diagnostic_lines,
        )


class MissingCountMetric(AggregationMetric):

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
        return self.missing_and_validity.get_sum_missing_count_expr(column_name)

    def convert_db_value(self, value) -> any:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        value = 0 if value is None else value
        return int(value)
