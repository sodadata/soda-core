from __future__ import annotations

import logging

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    Diagnostic,
    MeasuredNumericValueDiagnostic,
    Measurement,
)
from soda_core.contracts.impl.check_types.failed_rows_check_yaml import (
    FailedRowsCheckYaml,
)
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

        self.query_metric_impl: Optional[MetricImpl] = None
        if self.failed_rows_check_yaml.expression:
            self.query_metric_impl = self._resolve_metric(
                FailedRowsExpressionMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )

        elif self.failed_rows_check_yaml.query:
            self.query_metric_impl = self._resolve_metric(
                FailedRowsQueryMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )
            if contract_impl.data_source_impl:
                failed_rows_count_query: Query = FailedRowsCountQuery(
                    data_source_impl=contract_impl.data_source_impl,
                    metrics=[self.query_metric_impl],
                    failed_rows_query=self.failed_rows_check_yaml.query,
                )
                self.queries.append(failed_rows_count_query)

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        query_metric_value: Optional[Number] = (
            measurement_values.get_value(self.query_metric_impl) if self.query_metric_impl else None
        )

        diagnostics: list[Diagnostic] = []

        if isinstance(query_metric_value, Number):
            metric_name: str = "failed_rows_count"
            diagnostics.append(MeasuredNumericValueDiagnostic(name=metric_name, value=query_metric_value))

            if self.threshold:
                if self.threshold.passes(query_metric_value):
                    outcome = CheckOutcome.PASSED
                else:
                    outcome = CheckOutcome.FAILED

        return CheckResult(
            contract=contract,
            check=self._build_check_info(),
            metric_value=query_metric_value,
            outcome=outcome,
            diagnostics=diagnostics,
        )


class FailedRowsExpressionMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: FailedRowsCheckImpl,
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
        return COUNT(CASE_WHEN(SqlExpressionStr(self.expression), LITERAL(1)))

    def convert_db_value(self, value) -> any:
        return int(value) if value is not None else 0


class FailedRowsQueryMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: FailedRowsCheckImpl,
    ):
        self.query: str = check_impl.check_yaml.query
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=check_impl.type,
            check_filter=check_impl.check_yaml.filter,
        )

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = super()._get_id_properties()
        id_properties["query"] = self.query
        return id_properties


class FailedRowsCountQuery(Query):
    def __init__(self, data_source_impl: Optional[DataSourceImpl], metrics: list[MetricImpl], failed_rows_query: str):
        sql = data_source_impl.sql_dialect.build_select_sql(
            [WITH(alias="failed_rows").AS(cte_query=failed_rows_query), SELECT(COUNT(STAR())), FROM("failed_rows")]
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
