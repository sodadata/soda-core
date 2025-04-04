from __future__ import annotations

from typing import Optional

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    Diagnostic,
    NumericDiagnostic,
)
from soda_core.contracts.impl.check_types.missing_check_yaml import MissingCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetric
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    DerivedPercentageMetricImpl,
    MeasurementValues,
    MetricImpl,
    MissingAndValidity,
    MissingAndValidityCheckImpl,
    ThresholdImpl,
    ThresholdType,
)


class MissingCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["missing"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: MissingCheckYaml,
    ) -> Optional[CheckImpl]:
        return MissingCheck(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class MissingCheck(MissingAndValidityCheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: MissingCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
            default_threshold=ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be=0),
        )

        # TODO create better support in class hierarchy for common vs specific stuff.  name is common.  see other check type impls

        self.metric_name = "missing_percent" if check_yaml.metric == "percent" else "missing_count"
        self.missing_count_metric = self._resolve_metric(
            MissingCountMetric(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )

        self.row_count_metric_impl: MetricImpl = self._resolve_metric(
            RowCountMetric(
                contract_impl=contract_impl,
            )
        )

        self.missing_percent_metric_impl: MetricImpl = self.contract_impl.metrics_resolver.resolve_metric(
            DerivedPercentageMetricImpl(
                metric_type="missing_percent",
                fraction_metric_impl=self.missing_count_metric,
                total_metric_impl=self.row_count_metric_impl,
            )
        )

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        missing_count: int = measurement_values.get_value(self.missing_count_metric)
        diagnostics: list[Diagnostic] = [NumericDiagnostic(name="missing_count", value=missing_count)]

        row_count: int = measurement_values.get_value(self.row_count_metric_impl)
        diagnostics.append(NumericDiagnostic(name="row_count", value=row_count))
        missing_percent: float = measurement_values.get_value(self.missing_percent_metric_impl)
        diagnostics.append(NumericDiagnostic(name="missing_percent", value=missing_percent))

        threshold_value: Optional[Number] = missing_percent if self.metric_name == "missing_percent" else missing_count

        if self.threshold and isinstance(threshold_value, Number):
            if self.threshold.passes(threshold_value):
                outcome = CheckOutcome.PASSED
            else:
                outcome = CheckOutcome.FAILED

        return CheckResult(
            contract=contract,
            check=self._build_check_info(),
            metric_value=threshold_value,
            outcome=outcome,
            diagnostics=diagnostics,
        )


class MissingCountMetric(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MissingAndValidityCheckImpl,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=check_impl.type,
        )
        self.missing_and_validity: MissingAndValidity = check_impl.missing_and_validity

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column_impl.column_yaml.name
        return self.missing_and_validity.get_sum_missing_count_expr(column_name)

    def convert_db_value(self, value) -> any:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        value = 0 if value is None else value
        return int(value)
