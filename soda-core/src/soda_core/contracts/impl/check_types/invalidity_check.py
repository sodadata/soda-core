from __future__ import annotations

import logging

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    Diagnostic,
    Measurement,
    NumericDiagnostic,
)
from soda_core.contracts.impl.check_types.invalidity_check_yaml import InvalidCheckYaml
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
    Query,
    ThresholdImpl,
    ThresholdType,
    ValidReferenceData,
)

logger: logging.Logger = soda_logger


class InvalidCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["invalid"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: InvalidCheckYaml,
    ) -> Optional[CheckImpl]:
        return InvalidCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class InvalidCheckImpl(MissingAndValidityCheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: InvalidCheckYaml,
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

        if not self.missing_and_validity.has_validity_configurations():
            logger.error(
                msg="Invalid check does not have any valid or invalid configurations",
                extra={
                    ExtraKeys.LOCATION: self.check_yaml.check_yaml_object.location,
                },
            )

        # TODO create better support in class hierarchy for common vs specific stuff.  name is common.  see other check type impls

        self.metric_name = "invalid_percent" if check_yaml.metric == "percent" else "invalid_count"
        self.name = check_yaml.name if check_yaml.name else self.type

        self.invalid_count_metric_impl: Optional[MetricImpl] = None
        if self.missing_and_validity.has_reference_data():
            # noinspection PyTypeChecker
            self.invalid_count_metric_impl = self._resolve_metric(
                InvalidReferenceCountMetricImpl(
                    contract_impl=contract_impl, column_impl=column_impl, missing_and_validity=self.missing_and_validity
                )
            )
            self.queries.append(
                InvalidReferenceCountQuery(
                    metric_impl=self.invalid_count_metric_impl,
                    dataset_filter=self.contract_impl.filter,
                    check_filter=self.check_yaml.filter,
                    data_source_impl=contract_impl.data_source_impl,
                )
            )
        else:
            self.invalid_count_metric_impl = self._resolve_metric(
                InvalidCountMetric(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )

        self.row_count_metric = self._resolve_metric(RowCountMetric(contract_impl=contract_impl, check_impl=self))

        self.invalid_percent_metric = self._resolve_metric(
            DerivedPercentageMetricImpl(
                metric_type="invalid_percent",
                fraction_metric_impl=self.invalid_count_metric_impl,
                total_metric_impl=self.row_count_metric,
            )
        )

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        diagnostics: list[Diagnostic] = []
        invalid_count: int = measurement_values.get_value(self.invalid_count_metric_impl)
        if isinstance(invalid_count, Number):
            diagnostics.append(NumericDiagnostic(name="invalid_count", value=invalid_count))

        row_count: int = measurement_values.get_value(self.row_count_metric)
        invalid_percent: float = 0
        if isinstance(row_count, Number):
            diagnostics.append(NumericDiagnostic(name="row_count", value=row_count))

            if row_count > 0:
                invalid_percent = measurement_values.get_value(self.invalid_percent_metric)
            diagnostics.append(NumericDiagnostic(name="invalid_percent", value=invalid_percent))

        threshold_value: Optional[Number] = invalid_percent if self.metric_name == "invalid_percent" else invalid_count

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


class InvalidCountMetric(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MissingAndValidityCheckImpl,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type="invalid_count",
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=check_impl.missing_and_validity,
        )

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column_impl.column_yaml.name
        not_missing_and_invalid_expr = self.missing_and_validity.get_invalid_count_condition(column_name)
        invalid_count_condition: SqlExpression = (
            not_missing_and_invalid_expr
            if not self.check_filter
            else AND([SqlExpressionStr(self.check_filter), not_missing_and_invalid_expr])
        )
        return SUM(CASE_WHEN(invalid_count_condition, LITERAL(1), LITERAL(0)))

    def convert_db_value(self, value) -> any:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        value = 0 if value is None else value
        return int(value)


class InvalidReferenceCountMetricImpl(MetricImpl):
    def __init__(self, contract_impl: ContractImpl, column_impl: ColumnImpl, missing_and_validity: MissingAndValidity):
        super().__init__(
            contract_impl=contract_impl,
            metric_type="invalid_count",
            column_impl=column_impl,
            missing_and_validity=missing_and_validity,
        )


class InvalidReferenceCountQuery(Query):
    def __init__(
        self,
        metric_impl: InvalidReferenceCountMetricImpl,
        dataset_filter: Optional[str],
        check_filter: Optional[str],
        data_source_impl: Optional[DataSourceImpl],
    ):
        super().__init__(data_source_impl=data_source_impl, metrics=[metric_impl])

        valid_reference_data: ValidReferenceData = metric_impl.missing_and_validity.valid_reference_data

        referencing_dataset_name: str = metric_impl.contract_impl.dataset_name
        referencing_dataset_prefix: Optional[str] = metric_impl.contract_impl.dataset_prefix
        referencing_column_name: str = metric_impl.column_impl.column_yaml.name
        # C stands for the 'C'ontract dataset
        referencing_alias: str = "C"

        referenced_dataset_name: str = valid_reference_data.dataset_name
        referenced_dataset_prefix: Optional[list[str]] = (
            valid_reference_data.dataset_prefix
            if valid_reference_data.dataset_prefix is not None
            else metric_impl.contract_impl.dataset_prefix
        )
        referenced_column: str = valid_reference_data.column
        # R stands for the 'R'eference dataset
        referenced_alias: str = "R"

        # The variant to get the failed rows is:
        # SELECT(STAR().IN("C")),
        # which should translate to SELECT C.*

        is_missing_condition: SqlExpression = metric_impl.missing_and_validity.get_missing_count_condition(
            COLUMN(referencing_column_name).IN(referencing_alias)
        )
        is_invalid_value_condition: SqlExpression = metric_impl.missing_and_validity.get_invalid_count_condition(
            COLUMN(referencing_column_name).IN(referencing_alias)
        )
        is_invalid_reference_condition: SqlExpression = IS_NULL(COLUMN(referenced_column).IN(referenced_alias))

        is_invalid_condition: SqlExpression = (
            is_invalid_reference_condition
            if is_invalid_value_condition is None
            else OR([is_invalid_reference_condition, is_invalid_value_condition])
        )

        sql_ast: list = [
            SELECT(COUNT(STAR())),
            FROM(referencing_dataset_name).IN(referencing_dataset_prefix).AS(referencing_alias),
            LEFT_INNER_JOIN(referenced_dataset_name)
            .IN(referenced_dataset_prefix)
            .ON(
                EQ(
                    COLUMN(referencing_column_name).IN(referencing_alias),
                    COLUMN(referenced_column).IN(referenced_alias),
                )
            )
            .AS(referenced_alias),
            WHERE(AND([NOT(is_missing_condition), is_invalid_condition])),
        ]

        if dataset_filter or check_filter:
            dataset_filter_expr: Optional[SqlExpressionStr] = None
            check_filter_expr: Optional[SqlExpressionStr] = None
            combined_filter_expr: Optional[SqlExpression] = None

            if dataset_filter:
                dataset_filter_expr = SqlExpressionStr(dataset_filter)
                combined_filter_expr = dataset_filter_expr

            if check_filter:
                check_filter_expr = SqlExpressionStr(check_filter)
                combined_filter_expr = check_filter_expr

            if dataset_filter_expr and check_filter_expr:
                combined_filter_expr = AND([dataset_filter_expr, check_filter_expr])

            original_from = sql_ast[1].AS(None)
            sql_ast[1] = FROM("filtered_dataset").AS(referencing_alias)
            sql_ast = [
                WITH("filtered_dataset").AS([SELECT(STAR()), original_from, WHERE(combined_filter_expr)]),
            ] + sql_ast

        self.sql = self.data_source_impl.sql_dialect.build_select_sql(sql_ast)

    def execute(self) -> list[Measurement]:
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(self.sql)
        except Exception as e:
            logger.error(msg=f"Could not execute invalid reference query {self.sql}: {e}", exc_info=True)
            return []

        metric_value = query_result.rows[0][0]
        metric_impl: MetricImpl = self.metrics[0]
        return [Measurement(metric_id=metric_impl.id, value=metric_value, metric_name=metric_impl.type)]
