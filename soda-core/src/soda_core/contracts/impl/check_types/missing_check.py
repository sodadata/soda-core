from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckOutcome, CheckResult
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

    def setup_metrics(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: MissingCheckYaml,
    ):
        self.missing_count_metric_impl = self._resolve_metric(
            MissingCountMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )

        self.row_count_metric_impl: MetricImpl = self._resolve_metric(
            RowCountMetricImpl(contract_impl=contract_impl, check_impl=self)
        )

        self.missing_percent_metric_impl: MetricImpl = self.contract_impl.metrics_resolver.resolve_metric(
            DerivedPercentageMetricImpl(
                metric_type="missing_percent",
                fraction_metric_impl=self.missing_count_metric_impl,
                total_metric_impl=self.row_count_metric_impl,
            )
        )

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        missing_count: int = measurement_values.get_value(self.missing_count_metric_impl)
        row_count: int = measurement_values.get_value(self.row_count_metric_impl)
        missing_percent: float = measurement_values.get_value(self.missing_percent_metric_impl)

        diagnostic_metric_values: dict[str, float] = {
            "missing_count": missing_count,
            "missing_percent": missing_percent,
            "check_rows_tested": row_count,
            "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
        }

        threshold_value: Optional[Number] = missing_percent if self.metric_name == "missing_percent" else missing_count

        outcome = self.evaluate_threshold(threshold_value)

        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=threshold_value,
            diagnostic_metric_values=diagnostic_metric_values,
        )

    def get_threshold_metric_impl(self) -> Optional[MetricImpl]:
        return self.missing_count_metric_impl


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
            column_expression=check_impl.column_expression,
        )

    def sql_expression(self) -> SqlExpression:
        return SUM(CASE_WHEN(self.sql_condition_expression(), LITERAL(1)))

    def sql_condition_expression(self) -> SqlExpression:
        not_missing_and_invalid_expr = self.missing_and_validity.is_missing_expr(self.column_expression)
        return (
            not_missing_and_invalid_expr
            if not self.check_filter
            else AND([SqlExpressionStr(self.check_filter), not_missing_and_invalid_expr])
        )

    def convert_db_value(self, value) -> int:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        return int(value) if value is not None else 0
