from __future__ import annotations

import logging

from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    Diagnostic,
    NumericDiagnostic,
)
from soda_core.contracts.impl.check_types.duplicate_check_yaml import (
    ColumnDuplicateCheckYaml,
    DatasetDuplicateCheckYaml,
)
from soda_core.contracts.impl.check_types.invalidity_check_yaml import InvalidCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    DerivedMetricImpl,
    DerivedPercentageMetricImpl,
    MeasurementValues,
    MetricImpl,
    MissingAndValidity,
    MissingAndValidityCheckImpl,
    ThresholdImpl,
    ThresholdType,
    ValidCountMetric,
)

logger: logging.Logger = soda_logger


class DuplicateCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["duplicate"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: InvalidCheckYaml,
    ) -> Optional[CheckImpl]:
        if column_impl:
            return ColumnDuplicateCheckImpl(
                contract_impl=contract_impl,
                column_impl=column_impl,
                check_yaml=check_yaml,
            )
        else:
            return MultiColumnDuplicateCheckImpl(
                contract_impl=contract_impl,
                check_yaml=check_yaml,
            )


class ColumnDuplicateCheckImpl(MissingAndValidityCheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: ColumnDuplicateCheckYaml,
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

        self.metric_name = "duplicate_percent" if check_yaml.metric == "percent" else "duplicate_count"
        self.name = check_yaml.name if check_yaml.name else self.type

        self.distinct_count_metric_impl: MetricImpl = self._resolve_metric(
            ColumnDistinctCountMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )

        self.valid_count_metric_impl = self._resolve_metric(
            ValidCountMetric(
                contract_impl=contract_impl,
                column_impl=column_impl,
                check_impl=self,
            )
        )

        self.duplicate_count_metric_impl = self._resolve_metric(
            DuplicateCountMetricImpl(
                metric_type="duplicate_count",
                distinct_count_metric_impl=self.distinct_count_metric_impl,
                valid_count_metric_impl=self.valid_count_metric_impl,
                check_filter=self.check_yaml.filter,
                missing_and_validity=self.missing_and_validity,
            )
        )

        self.duplicate_percent_metric_impl = self._resolve_metric(
            DerivedPercentageMetricImpl(
                metric_type="duplicate_percent",
                fraction_metric_impl=self.duplicate_count_metric_impl,
                total_metric_impl=self.valid_count_metric_impl,
            )
        )

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        diagnostics: list[Diagnostic] = []

        distinct_count: int = measurement_values.get_value(self.distinct_count_metric_impl)
        if isinstance(distinct_count, Number):
            diagnostics.append(NumericDiagnostic(name="distinct_count", value=distinct_count))

        duplicate_count: int = measurement_values.get_value(self.duplicate_count_metric_impl)
        if isinstance(duplicate_count, Number):
            diagnostics.append(NumericDiagnostic(name="duplicate_count", value=duplicate_count))

        valid_count: int = measurement_values.get_value(self.valid_count_metric_impl)
        duplicate_percent: float = 0
        if isinstance(valid_count, Number):
            diagnostics.append(NumericDiagnostic(name="valid_count", value=valid_count))

            if valid_count > 0:
                duplicate_percent = measurement_values.get_value(self.duplicate_percent_metric_impl)
            diagnostics.append(NumericDiagnostic(name="duplicate_percent", value=duplicate_percent))

        threshold_value: Optional[Number] = (
            duplicate_percent if self.metric_name == "duplicate_percent" else duplicate_count
        )

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


class ColumnDistinctCountMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MissingAndValidityCheckImpl,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type="distinct_count",
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=check_impl.missing_and_validity,
        )

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column_impl.column_yaml.name

        filters: list = [SqlExpressionStr.optional(self.check_filter)]
        if self.missing_and_validity:
            filters.append(
                NOT(
                    OR.optional(
                        [
                            self.missing_and_validity.is_missing_expr(column_name),
                            self.missing_and_validity.is_invalid_expr(column_name),
                        ]
                    )
                )
            )
        filter_expr: Optional[SqlExpression] = AND.optional(filters)
        if filter_expr:
            return COUNT(DISTINCT(CASE_WHEN(filter_expr, column_name)))
        else:
            return COUNT(DISTINCT(column_name))

    def convert_db_value(self, value) -> int:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        return int(value) if value is not None else 0


class DuplicateCountMetricImpl(DerivedMetricImpl):
    def __init__(
        self,
        metric_type: str,
        distinct_count_metric_impl: MetricImpl,
        valid_count_metric_impl: MetricImpl,
        check_filter: Optional[str],
        missing_and_validity: Optional[MissingAndValidity],
    ):
        self.distinct_count_metric_impl: MetricImpl = distinct_count_metric_impl
        self.valid_count_metric_impl: MetricImpl = valid_count_metric_impl
        # Mind the ordering as the self._build_id() must come last
        super().__init__(
            contract_impl=distinct_count_metric_impl.contract_impl,
            column_impl=distinct_count_metric_impl.column_impl,
            metric_type=metric_type,
            check_filter=check_filter,
            missing_and_validity=missing_and_validity,
        )

    def get_metric_dependencies(self) -> list[MetricImpl]:
        return [self.distinct_count_metric_impl, self.valid_count_metric_impl]

    def compute_derived_value(self, measurement_values: MeasurementValues) -> Number:
        distinct_count: int = measurement_values.get_value(self.distinct_count_metric_impl)
        valid_count: int = measurement_values.get_value(self.valid_count_metric_impl)
        return valid_count - distinct_count


class MultiColumnDuplicateCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        check_yaml: DatasetDuplicateCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=None,
            check_yaml=check_yaml,
        )
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
            default_threshold=ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be=0),
        )

        self.metric_name = "duplicate_percent" if check_yaml.metric == "percent" else "duplicate_count"
        self.name = check_yaml.name if check_yaml.name else self.type

        self.multi_column_distinct_count_metric_impl: MetricImpl = self._resolve_metric(
            MultiColumnDistinctCountMetricImpl(contract_impl=contract_impl, check_impl=self)
        )

        self.row_count_metric_impl = self._resolve_metric(
            RowCountMetricImpl(
                contract_impl=contract_impl,
                check_impl=self,
            )
        )

        self.duplicate_count_metric_impl = self._resolve_metric(
            MultiColumnDuplicateCountMetricImpl(
                metric_type="duplicate_count",
                multi_column_distinct_count_metric_impl=self.multi_column_distinct_count_metric_impl,
                row_count_metric_impl=self.row_count_metric_impl,
                check_filter=self.check_yaml.filter,
            )
        )

        self.duplicate_percent_metric_impl = self._resolve_metric(
            DerivedPercentageMetricImpl(
                metric_type="duplicate_percent",
                fraction_metric_impl=self.duplicate_count_metric_impl,
                total_metric_impl=self.row_count_metric_impl,
            )
        )

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        diagnostics: list[Diagnostic] = []

        distinct_count: int = measurement_values.get_value(self.multi_column_distinct_count_metric_impl)
        if isinstance(distinct_count, Number):
            diagnostics.append(NumericDiagnostic(name="distinct_count", value=distinct_count))

        duplicate_count: int = measurement_values.get_value(self.duplicate_count_metric_impl)
        if isinstance(duplicate_count, Number):
            diagnostics.append(NumericDiagnostic(name="duplicate_count", value=duplicate_count))

        row_count: int = measurement_values.get_value(self.row_count_metric_impl)
        duplicate_percent: float = 0
        if isinstance(row_count, Number):
            diagnostics.append(NumericDiagnostic(name="row_count", value=row_count))

            if row_count > 0:
                duplicate_percent = measurement_values.get_value(self.duplicate_percent_metric_impl)
            diagnostics.append(NumericDiagnostic(name="duplicate_percent", value=duplicate_percent))

        threshold_value: Optional[Number] = (
            duplicate_percent if self.metric_name == "duplicate_percent" else duplicate_count
        )

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


class MultiColumnDistinctCountMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        check_impl: MultiColumnDuplicateCheckImpl,
    ):
        self.column_names: list[str] = check_impl.check_yaml.columns
        super().__init__(
            contract_impl=contract_impl,
            metric_type="distinct_count",
            check_filter=check_impl.check_yaml.filter,
        )

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, str] = super()._get_id_properties()
        if isinstance(self.column_names, list):
            for index, column_name in enumerate(self.column_names):
                id_properties[f"column[{index}]"] = column_name
        return id_properties

    def sql_expression(self) -> SqlExpression:
        filter_expr: SqlExpression = SqlExpressionStr.optional(self.check_filter)
        if filter_expr:
            return COUNT(DISTINCT(CASE_WHEN(filter_expr, TUPLE(self.column_names))))
        else:
            return COUNT(DISTINCT(self.column_names))

    def convert_db_value(self, value) -> int:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        return int(value) if value is not None else 0


class MultiColumnDuplicateCountMetricImpl(DerivedMetricImpl):
    def __init__(
        self,
        metric_type: str,
        multi_column_distinct_count_metric_impl: MetricImpl,
        row_count_metric_impl: MetricImpl,
        check_filter: Optional[str],
    ):
        self.multi_column_distinct_count_metric_impl: MetricImpl = multi_column_distinct_count_metric_impl
        self.row_count_metric_impl: MetricImpl = row_count_metric_impl
        # Mind the ordering as the self._build_id() must come last
        super().__init__(
            contract_impl=multi_column_distinct_count_metric_impl.contract_impl,
            column_impl=multi_column_distinct_count_metric_impl.column_impl,
            metric_type=metric_type,
            check_filter=check_filter,
        )

    def get_metric_dependencies(self) -> list[MetricImpl]:
        return [self.multi_column_distinct_count_metric_impl, self.row_count_metric_impl]

    def compute_derived_value(self, measurement_values: MeasurementValues) -> Number:
        distinct_count: int = measurement_values.get_value(self.multi_column_distinct_count_metric_impl)
        row_count: int = measurement_values.get_value(self.row_count_metric_impl)
        return row_count - distinct_count
