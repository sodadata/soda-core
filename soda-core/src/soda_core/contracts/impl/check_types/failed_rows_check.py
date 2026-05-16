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
    CheckCollectionImpl,
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
        check_collection_impl: CheckCollectionImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: FailedRowsCheckYaml,
    ) -> Optional[CheckImpl]:
        return FailedRowsCheckImpl(
            check_collection_impl=check_collection_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class FailedRowsCheckImpl(CheckImpl):
    def __init__(
        self,
        check_collection_impl: CheckCollectionImpl,
        column_impl: ColumnImpl,
        check_yaml: FailedRowsCheckYaml,
    ):
        super().__init__(
            check_collection_impl=check_collection_impl,
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
        check_collection_impl: CheckCollectionImpl,
        column_impl: ColumnImpl,
        check_yaml: FailedRowsCheckYaml,
    ):
        self.failed_rows_count_metric_impl: Optional[MetricImpl] = None
        self.check_rows_tested_metric_impl: Optional[MetricImpl] = None
        if self.is_expression_check():
            self.failed_rows_count_metric_impl = self._resolve_metric(
                FailedRowsExpressionMetricImpl(
                    check_collection_impl=check_collection_impl, column_impl=column_impl, check_impl=self
                )
            )
            self.check_rows_tested_metric_impl = self._resolve_metric(
                RowCountMetricImpl(check_collection_impl=check_collection_impl, check_impl=self)
            )

        elif self.is_query_check():
            self.failed_rows_count_metric_impl = self._resolve_metric(
                FailedRowsQueryMetricImpl(
                    check_collection_impl=check_collection_impl, column_impl=column_impl, check_impl=self
                )
            )

            sql = self.failed_rows_check_yaml.query

            if check_collection_impl.should_apply_sampling:
                sql = check_collection_impl.data_source_impl.sql_dialect.apply_sampling(
                    sql=sql,
                    sampler_limit=check_collection_impl.sampler_limit,
                    sampler_type=check_collection_impl.sampler_type,
                )
            if check_collection_impl.data_source_impl:
                failed_rows_count_query: Query = FailedRowsCountQuery(
                    data_source_impl=check_collection_impl.data_source_impl,
                    metrics=[self.failed_rows_count_metric_impl],
                    failed_rows_query=sql,
                )
                self.queries.append(failed_rows_count_query)

            if self.failed_rows_check_yaml.rows_tested_query and check_collection_impl.data_source_impl:
                self.check_rows_tested_metric_impl = self._resolve_metric(
                    RowCountMetricImpl(check_collection_impl=check_collection_impl, check_impl=self)
                )
                rows_tested_sql = self.failed_rows_check_yaml.rows_tested_query
                if check_collection_impl.should_apply_sampling:
                    rows_tested_sql = check_collection_impl.data_source_impl.sql_dialect.apply_sampling(
                        sql=rows_tested_sql,
                        sampler_limit=check_collection_impl.sampler_limit,
                        sampler_type=check_collection_impl.sampler_type,
                    )
                rows_tested_query: Query = RowsTestedQuery(
                    data_source_impl=check_collection_impl.data_source_impl,
                    metrics=[self.check_rows_tested_metric_impl],
                    rows_tested_sql=rows_tested_sql,
                )
                self.queries.append(rows_tested_query)

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        failed_rows_count: Optional[int] = measurement_values.get_value(self.failed_rows_count_metric_impl)

        diagnostic_metric_values: dict[str, Optional[Number]] = {}
        threshold_value: Optional[Number] = None

        if self.failed_rows_check_yaml.expression:
            threshold_value, diagnostic_metric_values = self._evaluate_with_rows_tested(
                failed_rows_count, measurement_values
            )

        elif self.failed_rows_check_yaml.query:
            if self.failed_rows_check_yaml.rows_tested_query:
                threshold_value, diagnostic_metric_values = self._evaluate_with_rows_tested(
                    failed_rows_count, measurement_values
                )
            else:
                threshold_value = failed_rows_count

                diagnostic_metric_values = {
                    # `failedRowsCount` is @NotNull in the FailedRows DTO — coalesce to 0
                    # in failure mode. `check_rows_tested` is nullable for this branch
                    # (no rows_tested_query specified).
                    "failed_rows_count": failed_rows_count if failed_rows_count is not None else 0,
                    "dataset_rows_tested": self.check_collection_impl.dataset_rows_tested,
                    "check_rows_tested": None,
                }

        outcome = self.evaluate_threshold(threshold_value)

        return CheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_value=threshold_value,
            diagnostic_metric_values=diagnostic_metric_values,
        )

    def _evaluate_with_rows_tested(
        self, failed_rows_count: Optional[int], measurement_values: MeasurementValues
    ) -> tuple[Optional[Number], dict[str, Optional[Number]]]:
        check_rows_tested: Optional[int] = measurement_values.get_value(self.check_rows_tested_metric_impl)
        # Default to 0 for diagnostics so consumers don't see null where they used to
        # see 0. `threshold_value` carries the "is this real" signal: it stays None
        # when dependencies are unmeasured, so evaluate_threshold → NOT_EVALUATED.
        failed_rows_percent: float = 0
        threshold_value: Optional[Number]
        if measurement_values.all_measured(self.failed_rows_count_metric_impl, self.check_rows_tested_metric_impl):
            failed_rows_percent = failed_rows_count * 100 / check_rows_tested if check_rows_tested > 0 else 0
            threshold_value = (
                failed_rows_percent if self.failed_rows_check_yaml.metric == "percent" else failed_rows_count
            )
        else:
            threshold_value = None if self.failed_rows_check_yaml.metric == "percent" else failed_rows_count

        diagnostic_metric_values: dict[str, Optional[Number]] = {
            # `failedRowsCount` is @NotNull in the FailedRows DTO — coalesce to 0
            # in failure mode. `check_rows_tested` is nullable per DTO.
            "failed_rows_count": failed_rows_count if failed_rows_count is not None else 0,
            "failed_rows_percent": failed_rows_percent,
            "dataset_rows_tested": self.check_collection_impl.dataset_rows_tested,
            "check_rows_tested": check_rows_tested,
        }
        return threshold_value, diagnostic_metric_values

    def get_threshold_metric_impl(self) -> Optional[MetricImpl]:
        return self.failed_rows_count_metric_impl

    def is_expression_check(self) -> bool:
        return self.failed_rows_check_yaml.expression

    def is_query_check(self) -> bool:
        return self.failed_rows_check_yaml.query


class FailedRowsExpressionMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        check_collection_impl: CheckCollectionImpl,
        column_impl: ColumnImpl,
        check_impl: FailedRowsCheckImpl,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.expression: str = check_impl.check_yaml.expression
        super().__init__(
            check_collection_impl=check_collection_impl,
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
        check_collection_impl: CheckCollectionImpl,
        column_impl: ColumnImpl,
        check_impl: FailedRowsCheckImpl,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
    ):
        self.query: str = check_impl.check_yaml.query
        super().__init__(
            check_collection_impl=check_collection_impl,
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


STREAMING_COUNT_WARNING_THRESHOLD = 10_000


class FailedRowsCountQuery(Query):
    def __init__(self, data_source_impl: Optional[DataSourceImpl], metrics: list[MetricImpl], failed_rows_query: str):
        self.failed_rows_query = failed_rows_query
        # Try CTE-wrapped COUNT first (efficient — only transfers a single number).
        # Store the raw query as self.sql for fallback and logging.
        count_sql = data_source_impl.sql_dialect.build_select_sql(
            [
                WITH([CTE(alias="failed_rows").AS(cte_query=failed_rows_query)]),
                SELECT(COUNT(STAR())),
                FROM("failed_rows"),
            ]
        )
        super().__init__(data_source_impl=data_source_impl, metrics=metrics, sql=count_sql)

    def execute(self) -> list[Measurement]:
        metric_value = None

        # Try CTE-wrapped COUNT(*) first — efficient, server-side aggregation.
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(self.sql)
            if query_result.rows:
                metric_value = query_result.rows[0][0]
        except Exception as e:
            logger.debug(f"CTE-wrapped count failed, falling back to row-by-row streaming: {e}")
            # Fallback: execute the raw user query and count rows one-by-one.
            try:
                row_count = 0

                def count_row(row, description):
                    nonlocal row_count
                    row_count += 1

                self.data_source_impl.execute_query_one_by_one(sql=self.failed_rows_query, row_callback=count_row)
                metric_value = row_count
                if metric_value > STREAMING_COUNT_WARNING_THRESHOLD:
                    logger.warning(
                        f"Streamed {metric_value} rows to count failed rows. "
                        f"The query could not be wrapped in a CTE and had to be executed directly. "
                        f"Consider rewriting your query so that it can be wrapped in a CTE."
                    )
            except Exception as e2:
                logger.error(
                    msg=f"Could not execute failed rows count query: \n{self.failed_rows_query}:\n{e2}",
                    exc_info=True,
                )
                return []

        metric_impl: MetricImpl = self.metrics[0]
        return [Measurement(metric_id=metric_impl.id, value=metric_value, metric_name=metric_impl.type)]


class RowsTestedQuery(Query):
    def __init__(self, data_source_impl: Optional[DataSourceImpl], metrics: list[MetricImpl], rows_tested_sql: str):
        super().__init__(data_source_impl=data_source_impl, metrics=metrics, sql=rows_tested_sql)

    def execute(self) -> list[Measurement]:
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(self.sql)
        except Exception as e:
            logger.error(msg=f"Could not execute rows tested query: \n{self.sql}:\n{e}", exc_info=True)
            return []

        if not query_result.rows:
            metric_value = None
        else:
            metric_value = query_result.rows[0][0]
        metric_impl: MetricImpl = self.metrics[0]
        return [Measurement(metric_id=metric_impl.id, value=metric_value, metric_name=metric_impl.type)]
