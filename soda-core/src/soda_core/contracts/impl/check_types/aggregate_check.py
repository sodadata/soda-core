from __future__ import annotations

import logging

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckOutcome, CheckResult
from soda_core.contracts.impl.check_types.aggregate_check_yaml import AggregateCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl
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

        # Check if the function is supported by the data source, if data source is available. It is not during `soda contract test`.
        if (
            self.function
            and self.contract_impl.data_source_impl
            and not contract_impl.data_source_impl.sql_dialect.supports_function(self.function)
        ):
            logger.error(
                msg=f"Aggregate function '{check_yaml.function}' is not supported on "
                f"'{contract_impl.data_source_impl.type_name}'",
                extra={ExtraKeys.LOCATION: check_yaml.check_yaml_object.create_location_from_yaml_dict_key("function")},
            )
            self.function = None

    def setup_metrics(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: AggregateCheckYaml,
    ):
        self.aggregate_metric = self._resolve_metric(
            AggregateFunctionMetricImpl(
                contract_impl=contract_impl, column_impl=column_impl, check_impl=self, function=self.function
            )
        )

        self.check_rows_tested_metric = self._resolve_metric(
            RowCountMetricImpl(contract_impl=contract_impl, check_impl=self)
        )

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        function_value: Optional[float | int] = measurement_values.get_value(self.aggregate_metric)
        check_rows_tested: int = measurement_values.get_value(self.check_rows_tested_metric)

        diagnostic_metric_values: dict[str, float] = {
            "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
            "check_rows_tested": check_rows_tested,
        }

        if isinstance(function_value, Number):
            diagnostic_metric_values[self.function] = function_value

            outcome = self.evaluate_threshold(function_value)

        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=function_value,
            diagnostic_metric_values=diagnostic_metric_values,
        )


class AggregateFunctionMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_impl: MissingAndValidityCheckImpl,
        function: Optional[str],
        column_name: Optional[str] = None,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.function: Optional[str] = function
        self.column_name = column_impl.column_yaml.name if column_impl else column_name
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=check_impl.type,
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=check_impl.missing_and_validity,
            data_source_impl=data_source_impl,
            dataset_identifier=dataset_identifier,
        )

    def sql_expression(self) -> SqlExpression:
        is_missing = self.missing_and_validity.is_missing_expr(self.column_name)
        is_invalid = self.missing_and_validity.is_invalid_expr(self.column_name)

        filters: list[SqlExpression] = []
        if is_missing or is_invalid:
            filters.append(NOT(AND.optional([is_missing, is_invalid])))
        if self.check_filter:
            filters.append(SqlExpressionStr(self.check_filter))

        arg: SqlExpression | str = self.column_name
        if filters:
            arg = CASE_WHEN(condition=AND(clauses=filters), if_expression=self.column_name, else_expression=None)

        return self.data_source_impl.sql_dialect.get_function_expression(self.function, arg)

    def convert_db_value(self, value) -> any:
        return float(value) if value is not None else None
