from __future__ import annotations

import logging

from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
)
from soda_core.contracts.impl.check_types.aggregate_check_yaml import AggregateCheckYaml
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    MeasurementValues,
    MissingAndValidityCheckImpl,
    ThresholdImpl,
)

logger: logging.Logger = soda_logger


class AggregateCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["aggregate"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: AggregateCheckYaml,
    ) -> Optional[CheckImpl]:
        return AggregateCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class AggregateCheckImpl(MissingAndValidityCheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: AggregateCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
        )

        self.function: str = check_yaml.function.lower()

        if self.function and not contract_impl.data_source_impl.sql_dialect.supports_function(self.function):
            logger.error(
                msg=f"Aggregate function '{check_yaml.function}' is not supported on "
                f"'{contract_impl.data_source_impl.type_name}'",
                extra={ExtraKeys.LOCATION: check_yaml.check_yaml_object.create_location_from_yaml_dict_key("function")},
            )
            self.function = None

        self.aggregate_metric = self._resolve_metric(
            AggregateFunctionMetricImpl(
                contract_impl=contract_impl, column_impl=column_impl, check_impl=self, function=self.function
            )
        )

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        function_value: Optional[Number] = measurement_values.get_value(self.aggregate_metric)
        diagnostic_metric_values: dict[str, float] = {}

        if isinstance(function_value, Number):
            diagnostic_metric_values[self.function] = function_value

            if self.threshold:
                if self.threshold.passes(function_value):
                    outcome = CheckOutcome.PASSED
                else:
                    outcome = CheckOutcome.FAILED

        return CheckResult(
            contract=contract,
            check=self._build_check_info(),
            outcome=outcome,
            threshold_metric_name=self.function,
            diagnostic_metric_values=diagnostic_metric_values,
        )


class AggregateFunctionMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MissingAndValidityCheckImpl,
        function: Optional[str],
    ):
        self.function: Optional[str] = function
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=check_impl.type,
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=check_impl.missing_and_validity,
        )

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column_impl.column_yaml.name

        is_missing = self.missing_and_validity.is_missing_expr(column_name)
        is_invalid = self.missing_and_validity.is_invalid_expr(column_name)

        arg: SqlExpression | str = column_name
        if is_missing or is_invalid:
            arg = CASE_WHEN(NOT(AND.optional([is_missing, is_invalid])), column_name)

        return FUNCTION(name=self.function, args=[arg])

    def convert_db_value(self, value) -> any:
        return float(value) if value is not None else None
