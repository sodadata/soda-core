from __future__ import annotations

import logging

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckOutcome, CheckResult
from soda_core.contracts.impl.check_types.duplicate_check_yaml import (
    ColumnDuplicateCheckYaml,
    MultiColumnDuplicateCheckYaml,
)
from soda_core.contracts.impl.check_types.invalidity_check_yaml import InvalidCheckYaml
from soda_core.contracts.impl.check_types.missing_check import MissingCountMetricImpl
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

    def setup_metrics(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: ColumnDuplicateCheckYaml,
    ):
        self.distinct_count_metric_impl: MetricImpl = self._resolve_metric(
            ColumnDistinctCountMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )

        self.check_rows_tested_metric_impl = self._resolve_metric(
            RowCountMetricImpl(
                contract_impl=contract_impl,
                check_impl=self,
            )
        )

        self.missing_count_metric_impl = self._resolve_metric(
            MissingCountMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )

        self.duplicate_count_metric_impl = self._resolve_metric(
            DuplicateCountMetricImpl(
                metric_type="duplicate_count",
                distinct_count_metric_impl=self.distinct_count_metric_impl,
                valid_count_metric_impl=self.check_rows_tested_metric_impl,
                check_filter=self.check_yaml.filter,
                missing_and_validity=self.missing_and_validity,
            )
        )

        self.duplicate_percent_metric_impl = self._resolve_metric(
            DerivedPercentageMetricImpl(
                metric_type="duplicate_percent",
                fraction_metric_impl=self.duplicate_count_metric_impl,
                total_metric_impl=self.check_rows_tested_metric_impl,
            )
        )

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        diagnostic_metric_values: dict[str, float] = {"dataset_rows_tested": self.contract_impl.dataset_rows_tested}

        distinct_count: int = measurement_values.get_value(self.distinct_count_metric_impl)

        check_rows_tested_count: int = measurement_values.get_value(self.check_rows_tested_metric_impl)
        if isinstance(check_rows_tested_count, Number):
            diagnostic_metric_values["check_rows_tested"] = check_rows_tested_count

        missing_count: int = measurement_values.get_value(self.missing_count_metric_impl)
        if isinstance(missing_count, Number):
            diagnostic_metric_values["missing_count"] = missing_count

        duplicate_count: int = 0
        duplicate_percent: float = 0
        if (
            isinstance(check_rows_tested_count, Number)
            and isinstance(missing_count, Number)
            and isinstance(distinct_count, Number)
        ):
            duplicate_count: int = check_rows_tested_count - missing_count - distinct_count
            diagnostic_metric_values["duplicate_count"] = duplicate_count

            non_missing_total: int = check_rows_tested_count - missing_count
            if non_missing_total > 0:
                duplicate_percent: float = duplicate_count * 100 / non_missing_total
            diagnostic_metric_values["duplicate_percent"] = duplicate_percent

        threshold_value: Optional[Number] = (
            duplicate_percent if self.metric_name == "duplicate_percent" else duplicate_count
        )

        outcome = self.evaluate_threshold(threshold_value)

        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=threshold_value,
            diagnostic_metric_values=diagnostic_metric_values,
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
            column_expression=check_impl.column_expression,
        )

    def sql_expression(self) -> SqlExpression:
        column_expression: COLUMN | SqlExpressionStr = self.column_expression
        filters: list[SqlExpression] = [SqlExpressionStr.optional(self.check_filter)]
        if self.missing_and_validity:
            filters.append(
                NOT(
                    OR.optional(
                        [
                            self.missing_and_validity.is_missing_expr(column_expression),
                        ]
                    )
                )
            )
        filter_expr: Optional[SqlExpression] = AND.optional(filters)
        if filter_expr:
            return COUNT(DISTINCT(CASE_WHEN(filter_expr, column_expression)))
        else:
            return COUNT(DISTINCT(column_expression))

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
        check_yaml: MultiColumnDuplicateCheckYaml,
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

    def setup_metrics(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: MultiColumnDuplicateCheckYaml,
    ):
        self.metric_name = "duplicate_percent" if check_yaml.metric == "percent" else "duplicate_count"

        self.multi_column_distinct_count_metric_impl: MetricImpl = self._resolve_metric(
            MultiColumnDistinctCountMetricImpl(
                contract_impl=contract_impl, check_impl=self, column_names=check_yaml.columns
            )
        )

        self.row_count_metric_impl = self._resolve_metric(
            RowCountMetricImpl(
                contract_impl=contract_impl,
                check_impl=self,
            )
        )

        # self.duplicate_count_metric_impl = self._resolve_metric(
        #     MultiColumnDuplicateCountMetricImpl(
        #         metric_type="duplicate_count",
        #         multi_column_distinct_count_metric_impl=self.multi_column_distinct_count_metric_impl,
        #         row_count_metric_impl=self.row_count_metric_impl,
        #         check_filter=self.check_yaml.filter,
        #     )
        # )
        #
        # self.duplicate_percent_metric_impl = self._resolve_metric(
        #     DerivedPercentageMetricImpl(
        #         metric_type="duplicate_percent",
        #         fraction_metric_impl=self.duplicate_count_metric_impl,
        #         total_metric_impl=self.row_count_metric_impl,
        #     )
        # )

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        row_count: int = measurement_values.get_value(self.row_count_metric_impl)
        distinct_count: int = measurement_values.get_value(self.multi_column_distinct_count_metric_impl)
        duplicate_count: int = 0
        duplicate_percent: float = 0

        if isinstance(row_count, Number) and isinstance(distinct_count, Number):
            duplicate_count = row_count - distinct_count
            if row_count > 0:
                duplicate_percent = duplicate_count * 100 / row_count

        diagnostic_metric_values: dict[str, float] = {
            "duplicate_count": duplicate_count,
            "duplicate_percent": duplicate_percent,
            "check_rows_tested": row_count,
            "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
        }

        threshold_value: Optional[Number] = (
            duplicate_percent if self.metric_name == "duplicate_percent" else duplicate_count
        )

        outcome = self.evaluate_threshold(threshold_value)

        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=threshold_value,
            diagnostic_metric_values=diagnostic_metric_values,
        )


class MultiColumnDistinctCountMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        check_impl: MultiColumnDuplicateCheckImpl,
        column_names: list[str],
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.column_names: list[str] = column_names
        super().__init__(
            contract_impl=contract_impl,
            metric_type="distinct_count",
            check_filter=check_impl.check_yaml.filter,
            data_source_impl=data_source_impl,
            dataset_identifier=dataset_identifier,
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
            return COUNT(DISTINCT(CASE_WHEN(filter_expr, COMBINED_HASH(self.column_names))))
        else:
            return COUNT(DISTINCT(COMBINED_HASH(self.column_names)))

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
