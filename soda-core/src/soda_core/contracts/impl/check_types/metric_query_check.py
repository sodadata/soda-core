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
from soda_core.contracts.impl.check_types.metric_query_check_yaml import (
    MetricQueryCheckYaml,
)
from soda_core.contracts.impl.contract_verification_impl import (
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


class MetricQueryCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["metric_query"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: MetricQueryCheckYaml,
    ) -> Optional[CheckImpl]:
        return MetricQueryCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class MetricQueryCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: MetricQueryCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )
        self.metric_query_check_yaml: MetricQueryCheckYaml = check_yaml
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
        )
        self.query_metric_impl = self._resolve_metric(
            MetricQueryMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
        )
        if contract_impl.data_source_impl:
            metric_query: Query = MetricQuery(
                data_source_impl=contract_impl.data_source_impl,
                metrics=[self.query_metric_impl],
                sql=self.metric_query_check_yaml.query,
            )
            self.queries.append(metric_query)

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        query_metric_value: Optional[Number] = measurement_values.get_value(self.query_metric_impl)
        diagnostics: list[Diagnostic] = []

        if isinstance(query_metric_value, Number):
            diagnostics.append(
                MeasuredNumericValueDiagnostic(name=self.metric_query_check_yaml.metric, value=query_metric_value)
            )

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


class MetricQueryMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_impl: MetricQueryCheckImpl,
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
