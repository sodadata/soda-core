from __future__ import annotations

import logging

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Measurement,
)
from soda_core.contracts.impl.check_types.failed_rows_check_yaml import (
    FailedRowsCheckYaml,
)
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    MeasurementValues,
    MetricImpl,
    Query,
    ThresholdImpl,
    ThresholdType,
)

logger: logging.Logger = soda_logger


class FailedRowsCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["failed_rows"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: FailedRowsCheckYaml,
    ) -> Optional[CheckImpl]:
        return FailedRowsCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class FailedRowsCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: FailedRowsCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )
        self.failed_rows_check_yaml: FailedRowsCheckYaml = check_yaml
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
            default_threshold=ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be=0),
        )

    def setup_metrics(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: FailedRowsCheckYaml,
    ):
        self.failed_rows_count_metric_impl: Optional[MetricImpl] = None
        if self.is_expression_check():
            self.failed_rows_count_metric_impl = self._resolve_metric(
                FailedRowsExpressionMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )
            self.check_rows_tested_metric_impl: MetricImpl = self._resolve_metric(
                RowCountMetricImpl(contract_impl=contract_impl, check_impl=self)
            )

        elif self.is_query_check():
            self.failed_rows_count_metric_impl = self._resolve_metric(
                FailedRowsQueryMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )

            sql = self.failed_rows_check_yaml.query

            if contract_impl.should_apply_sampling:
                sql = contract_impl.data_source_impl.sql_dialect.apply_sampling(
                    sql=sql,
                    sampler_limit=contract_impl.sampler_limit,
                    sampler_type=contract_impl.sampler_type,
                )
            if contract_impl.data_source_impl:
                failed_rows_count_query: Query = FailedRowsCountQuery(
                    data_source_impl=contract_impl.data_source_impl,
                    metrics=[self.failed_rows_count_metric_impl],
                    failed_rows_query=sql,
                )
                self.queries.append(failed_rows_count_query)

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        failed_rows_count: Optional[int] = measurement_values.get_value(self.failed_rows_count_metric_impl)

        diagnostic_metric_values: dict[str, float] = {}
        threshold_value: Optional[float] = None

        if self.failed_rows_check_yaml.expression:
            check_rows_tested: Optional[int] = measurement_values.get_value(self.check_rows_tested_metric_impl)
            failed_rows_percent: Optional[float] = 0
            if (
                isinstance(check_rows_tested, Number)
                and isinstance(failed_rows_count, Number)
                and check_rows_tested > 0
            ):
                failed_rows_percent = failed_rows_count * 100 / check_rows_tested

            if self.failed_rows_check_yaml.metric == "percent":
                threshold_value = failed_rows_percent
            else:
                threshold_value = failed_rows_count

            diagnostic_metric_values = {
                "failed_rows_count": failed_rows_count,
                "failed_rows_percent": failed_rows_percent,
                "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
                "check_rows_tested": check_rows_tested,
            }

        elif self.failed_rows_check_yaml.query:
            threshold_value = failed_rows_count

            diagnostic_metric_values = {
                "failed_rows_count": failed_rows_count,
                "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
            }

        outcome = self.evaluate_threshold(threshold_value)

        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=threshold_value,
            diagnostic_metric_values=diagnostic_metric_values,
        )

    def get_threshold_metric_impl(self) -> Optional[MetricImpl]:
        return self.failed_rows_count_metric_impl

    def is_expression_check(self) -> bool:
        return self.failed_rows_check_yaml.expression

    def is_query_check(self) -> bool:
        return self.failed_rows_check_yaml.query


class FailedRowsExpressionMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: FailedRowsCheckImpl,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.expression: str = check_impl.check_yaml.expression
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=check_impl.type,
            check_filter=check_impl.check_yaml.filter,
            data_source_impl=data_source_impl,
            dataset_identifier=dataset_identifier,
        )

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = super()._get_id_properties()
        id_properties["expression"] = self.expression
        return id_properties

    def sql_expression(self) -> SqlExpression:
        return COUNT(CASE_WHEN(SqlExpressionStr(self.expression), LITERAL(1)))

    def sql_condition_expression(self) -> SqlExpression:
        return SqlExpressionStr(self.expression)

    def convert_db_value(self, value) -> any:
        return int(value) if value is not None else 0


class FailedRowsQueryMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: FailedRowsCheckImpl,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.query: str = check_impl.check_yaml.query
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=check_impl.type,
            check_filter=check_impl.check_yaml.filter,
            data_source_impl=data_source_impl,
            dataset_identifier=dataset_identifier,
        )

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = super()._get_id_properties()
        id_properties["query"] = self.query
        return id_properties


class FailedRowsCountQuery(Query):
    def __init__(self, data_source_impl: Optional[DataSourceImpl], metrics: list[MetricImpl], failed_rows_query: str):
        sql = data_source_impl.sql_dialect.build_select_sql(
            [
                WITH([CTE(alias="failed_rows").AS(cte_query=failed_rows_query)]),
                SELECT(COUNT(STAR())),
                FROM("failed_rows"),
            ]
        )
        super().__init__(data_source_impl=data_source_impl, metrics=metrics, sql=sql)

    def execute(self) -> list[Measurement]:
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(self.sql)
        except Exception as e:
            logger.error(msg=f"Could not execute failed rows count query: \n{self.sql}:\n{e}", exc_info=True)
            return []

        metric_value = query_result.rows[0][0]
        metric_impl: MetricImpl = self.metrics[0]
        return [Measurement(metric_id=metric_impl.id, value=metric_value, metric_name=metric_impl.type)]
