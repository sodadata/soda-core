from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    Diagnostic,
    MetricValuesDiagnostic,
)
from soda_core.contracts.impl.check_types.missing_check_yaml import MissingCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    DerivedPercentageMetricImpl,
    MeasurementValues,
    MetricImpl,
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
        return MissingCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class MissingCheckImpl(MissingAndValidityCheckImpl):
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

        self.metric_name = "missing_percent" if check_yaml.metric == "percent" else "missing_count"
        self.missing_count_metric = self._resolve_metric(
            MissingCountMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )

        self.row_count_metric_impl: MetricImpl = self._resolve_metric(
            RowCountMetricImpl(contract_impl=contract_impl, check_impl=self)
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
        diagnostics: list[Diagnostic] = [MetricValuesDiagnostic(name="missing_count", value=missing_count)]

        row_count: int = measurement_values.get_value(self.row_count_metric_impl)
        diagnostics.append(MetricValuesDiagnostic(name="row_count", value=row_count))
        missing_percent: float = measurement_values.get_value(self.missing_percent_metric_impl)
        diagnostics.append(MetricValuesDiagnostic(name="missing_percent", value=missing_percent))

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


class MissingCountMetricImpl(AggregationMetricImpl):
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
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=check_impl.missing_and_validity,
        )

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column_impl.column_yaml.name
        not_missing_and_invalid_expr = self.missing_and_validity.is_missing_expr(column_name)
        missing_count_condition: SqlExpression = (
            not_missing_and_invalid_expr
            if not self.check_filter
            else AND([SqlExpressionStr(self.check_filter), not_missing_and_invalid_expr])
        )
        return SUM(CASE_WHEN(missing_count_condition, LITERAL(1), LITERAL(0)))

    def convert_db_value(self, value) -> int:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        return int(value) if value is not None else 0
