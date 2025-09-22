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
from soda_core.contracts.impl.check_types.metric_check_yaml import MetricCheckYaml
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
)

logger: logging.Logger = soda_logger


class MetricCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["metric"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: MetricCheckYaml,
    ) -> Optional[CheckImpl]:
        return MetricCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class MetricCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: MetricCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )
        self.metric_check_yaml: MetricCheckYaml = check_yaml
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
        )

        self.numeric_metric_impl: Optional[MetricImpl] = None
        if self.metric_check_yaml.expression:
            self.numeric_metric_impl = self._resolve_metric(
                MetricExpressionMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )
            self.check_rows_tested_metric_impl: MetricImpl = self._resolve_metric(
                RowCountMetricImpl(contract_impl=contract_impl, check_impl=self)
            )

        elif self.metric_check_yaml.query:
            self.numeric_metric_impl = self._resolve_metric(
                MetricQueryMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )
            if contract_impl.data_source_impl:
                metric_query: Query = MetricQuery(
                    data_source_impl=contract_impl.data_source_impl,
                    metrics=[self.numeric_metric_impl],
                    sql=self.metric_check_yaml.query,
                )
                self.queries.append(metric_query)

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        numeric_metric_value: Optional[Number] = measurement_values.get_value(self.numeric_metric_impl)

        diagnostic_metric_values: dict[str, float] = {}

        if self.metric_check_yaml.expression:
            diagnostic_metric_values: dict[str, float] = {
                "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
            }
        elif self.metric_check_yaml.query:
            # TODO delete this after
            # https://linear.app/sodadata/issue/DTL-922/fix-backend-diagnostics-mismatches-from-core
            # is done
            diagnostic_metric_values: dict[str, float] = {
                "dataset_rows_tested": 0,
            }

        outcome = self.evaluate_threshold(numeric_metric_value)

        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=numeric_metric_value,
            diagnostic_metric_values=diagnostic_metric_values,
        )


class MetricExpressionMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MetricCheckImpl,
        expression: str | None = None,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.expression: str = expression or check_impl.check_yaml.expression
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
        return SqlExpressionStr(self.expression)

    def convert_db_value(self, value) -> any:
        return float(value) if value is not None else None


class MetricQueryMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MetricCheckImpl,
        query: Optional[str] = None,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.query: str = query or check_impl.check_yaml.query
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


class MetricQuery(Query):
    def __init__(self, data_source_impl: Optional[DataSourceImpl], metrics: list[MetricImpl], sql: str):
        super().__init__(data_source_impl=data_source_impl, metrics=metrics, sql=sql)

    def build_sql(self) -> str:
        return super().build_sql()

    def execute(self) -> list[Measurement]:
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(self.sql)
        except Exception as e:
            logger.error(msg=f"Could not execute metric query: \n{self.sql}:\n{e}", exc_info=True)
            return []

        metric_value = query_result.rows[0][0]
        metric_impl: MetricImpl = self.metrics[0]
        return [Measurement(metric_id=metric_impl.id, value=metric_value, metric_name=metric_impl.type)]
