from __future__ import annotations

import logging

from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    Diagnostic,
    MeasuredNumericValueDiagnostic,
)
from soda_core.contracts.impl.check_types.metric_expression_check_yaml import (
    MetricExpressionCheckYaml,
)
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    MeasurementValues,
    ThresholdImpl,
)

logger: logging.Logger = soda_logger


class MetricExpressionCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["metric_expression"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: MetricExpressionCheckYaml,
    ) -> Optional[CheckImpl]:
        return MetricExpressionCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class MetricExpressionCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: MetricExpressionCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )
        self.metric_expression_check_yaml: MetricExpressionCheckYaml = check_yaml
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
        )
        self.expression_metric = self._resolve_metric(
            MetricExpressionMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        expression_metric_value: Optional[Number] = measurement_values.get_value(self.expression_metric)
        diagnostics: list[Diagnostic] = []

        if isinstance(expression_metric_value, Number):
            diagnostics.append(MeasuredNumericValueDiagnostic(name="expression_value", value=expression_metric_value))

            if self.threshold:
                if self.threshold.passes(expression_metric_value):
                    outcome = CheckOutcome.PASSED
                else:
                    outcome = CheckOutcome.FAILED

        return CheckResult(
            contract=contract,
            check=self._build_check_info(),
            metric_value=expression_metric_value,
            outcome=outcome,
            diagnostics=diagnostics,
        )


class MetricExpressionMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MetricExpressionCheckImpl,
    ):
        self.expression: str = check_impl.check_yaml.expression
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=check_impl.type,
            check_filter=check_impl.check_yaml.filter,
        )

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = super()._get_id_properties()
        id_properties["expression"] = self.expression
        return id_properties

    def sql_expression(self) -> SqlExpression:
        return SqlExpressionStr(self.expression)

    def convert_db_value(self, value) -> any:
        return float(value) if value is not None else None
