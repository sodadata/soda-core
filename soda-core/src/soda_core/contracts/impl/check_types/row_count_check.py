from __future__ import annotations

from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    Diagnostic,
    NumericDiagnostic,
)
from soda_core.contracts.impl.check_types.row_count_check_yaml import RowCountCheckYaml
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    MeasurementValues,
    ThresholdImpl,
    ThresholdType,
)


class RowCountCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["row_count"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: RowCountCheckYaml,
    ) -> Optional[CheckImpl]:
        return RowCountCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class RowCountCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: RowCountCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )

        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
            default_threshold=ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be_greater_than=0),
        )

        metric_name: str = ThresholdImpl.get_metric_name(check_yaml.type_name, column_impl=column_impl)
        self.summary = (
            self.threshold.get_assertion_summary(metric_name=metric_name)
            if self.threshold
            else f"{check_yaml.type_name} (invalid threshold)"
        )

        self.row_count_metric = self._resolve_metric(RowCountMetricImpl(contract_impl=contract_impl, check_impl=self))

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED
        row_count: int = measurement_values.get_value(self.row_count_metric)

        if self.threshold:
            if self.threshold.passes(row_count):
                outcome = CheckOutcome.PASSED
            else:
                outcome = CheckOutcome.FAILED

        diagnostics: list[Diagnostic] = [NumericDiagnostic(name="row_count", value=row_count)]

        return CheckResult(
            contract=contract,
            check=self._build_check_info(),
            metric_value=row_count,
            outcome=outcome,
            diagnostics=diagnostics,
        )


class RowCountMetricImpl(AggregationMetricImpl):
    def __init__(self, contract_impl: ContractImpl, check_impl: CheckImpl):
        super().__init__(
            contract_impl=contract_impl,
            metric_type="row_count",
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=None,
        )

    def sql_expression(self) -> SqlExpression:
        if self.check_filter:
            return SUM(CASE_WHEN(SqlExpressionStr(self.check_filter), LITERAL(1)))
        else:
            return COUNT(STAR())

    def convert_db_value(self, value: any) -> any:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        return int(value) if value is not None else 0
