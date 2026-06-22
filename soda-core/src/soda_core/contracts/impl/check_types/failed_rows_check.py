from __future__ import annotations

import logging

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckResult, Measurement
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
        self.check_rows_tested_metric_impl: Optional[MetricImpl] = None
        if self.is_expression_check():
            self.failed_rows_count_metric_impl = self._resolve_metric(
                FailedRowsExpressionMetricImpl(contract_impl=contract_impl, column_impl=column_impl, check_impl=self)
            )
            self.check_rows_tested_metric_impl = self._resolve_metric(
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

            if self.failed_rows_check_yaml.rows_tested_query and contract_impl.data_source_impl:
                # Must NOT use RowCountMetricImpl here — its identity hash
                # would match the contract-wide row_count metric and the
                # custom-query measurement would clobber dataset_rows_tested.
                self.check_rows_tested_metric_impl = self._resolve_metric(
                    RowsTestedQueryMetricImpl(contract_impl=contract_impl, check_impl=self)
                )
                rows_tested_sql = self.failed_rows_check_yaml.rows_tested_query
                if contract_impl.should_apply_sampling:
                    rows_tested_sql = contract_impl.data_source_impl.sql_dialect.apply_sampling(
                        sql=rows_tested_sql,
                        sampler_limit=contract_impl.sampler_limit,
                        sampler_type=contract_impl.sampler_type,
                    )
                rows_tested_query: Query = RowsTestedQuery(
                    data_source_impl=contract_impl.data_source_impl,
                    metrics=[self.check_rows_tested_metric_impl],
                    rows_tested_sql=rows_tested_sql,
                )
                self.queries.append(rows_tested_query)

    def get_required_metric_impls(self) -> list[MetricImpl]:
        # failed_rows_count is the threshold value for `metric: count` and the
        # numerator for `metric: percent`. check_rows_tested is the denominator for
        # percent; for count it's a nullable diagnostic (rows_tested_query may
        # legitimately return NULL), so we do NOT require it on that path.
        #
        # Both metric impls are Optional — setup_metrics only creates them when the
        # YAML provides `expression` or `query` (or `rows_tested_query`). If the
        # YAML provides none (malformed config the parser warned about but accepted),
        # we return an empty list so the framework skips gating; evaluate() then
        # produces a None threshold_value via its bare-query fallback, and
        # evaluate_threshold(None) drives NOT_EVALUATED — no `all_measured(None)`
        # crash on `get_value(None).id`.
        required: list[MetricImpl] = []
        if self.failed_rows_count_metric_impl is not None:
            required.append(self.failed_rows_count_metric_impl)
        if self.failed_rows_check_yaml.metric == "percent" and self.check_rows_tested_metric_impl is not None:
            required.append(self.check_rows_tested_metric_impl)
        return required

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        failed_rows_count: int = measurement_values.get_value(self.failed_rows_count_metric_impl)

        # Use `check_rows_tested_metric_impl is not None` rather than reading
        # `rows_tested_query` directly: the YAML parser only *warns* (not rejects)
        # when rows_tested_query is supplied without a query, so the YAML field can
        # be truthy with no corresponding metric. Gating on the metric impl avoids
        # a `get_value(None) -> AttributeError` crash in that pathological config.
        if self.check_rows_tested_metric_impl is not None:
            threshold_value, diagnostic_metric_values = self._evaluate_with_rows_tested(
                failed_rows_count, measurement_values
            )
        else:
            # Bare query branch: no rows_tested_query, so check_rows_tested is unknown.
            threshold_value = failed_rows_count
            diagnostic_metric_values = {
                "failed_rows_count": failed_rows_count,
                "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
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
        self, failed_rows_count: int, measurement_values: MeasurementValues
    ) -> tuple[Number, dict[str, Optional[Number]]]:
        # check_rows_tested may be None (rows_tested_query returned NULL) on the
        # count-metric path. We deliberately do NOT require it for that path
        # (see get_required_metric_impls). For the percent path the framework
        # gates upstream, so check_rows_tested is guaranteed non-None here.
        check_rows_tested: Optional[int] = measurement_values.get_value(self.check_rows_tested_metric_impl)
        failed_rows_percent: float = (
            failed_rows_count * 100 / check_rows_tested
            if isinstance(check_rows_tested, Number) and check_rows_tested > 0
            else 0
        )
        threshold_value: Number = (
            failed_rows_percent if self.failed_rows_check_yaml.metric == "percent" else failed_rows_count
        )
        diagnostic_metric_values: dict[str, Optional[Number]] = {
            "failed_rows_count": failed_rows_count,
            "failed_rows_percent": failed_rows_percent,
            "dataset_rows_tested": self.contract_impl.dataset_rows_tested,
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
        # Route the count through sql_condition_expression() so the numerator and the failing-rows
        # condition always carry the same check_filter (they cannot drift). Mirrors
        # MissingCountMetricImpl / InvalidCountMetricImpl.
        return COUNT(CASE_WHEN(self.sql_condition_expression(), LITERAL(1)))

    def sql_condition_expression(self) -> SqlExpression:
        # Apply the check-level filter, like missing/invalid do. When check_filter is None this
        # collapses to the bare expression (byte-identical to the previous behaviour), so it is a
        # no-op for filterless checks and only scopes checks that actually set `filter:`.
        return AND.optional(
            [
                SqlExpressionStr.optional(self.check_filter),
                SqlExpressionStr(self.expression),
            ]
        )

    def convert_db_value(self, value) -> any:
        return int(value) if value is not None else 0


class RowsTestedQueryMetricImpl(MetricImpl):
    """Row count from a user-provided rows_tested_query. SQL is part of the
    identity so it can't collide with the contract row_count metric, and two
    checks with the same SQL still share one measurement. Not an
    AggregationMetricImpl: the value comes from the user's query, not COUNT(*).
    """

    def __init__(self, contract_impl: ContractImpl, check_impl: FailedRowsCheckImpl):
        self.rows_tested_query: str = check_impl.failed_rows_check_yaml.rows_tested_query
        super().__init__(
            contract_impl=contract_impl,
            metric_type="rows_tested_query",
            check_filter=check_impl.check_yaml.filter,
        )

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = super()._get_id_properties()
        id_properties["rows_tested_query"] = self.rows_tested_query
        return id_properties

    def sql_condition_expression(self) -> Optional[SqlExpression]:
        return None


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

                # Deliberately the DEFAULT (buffered) variant, not the
                # memory-optimized one: this fallback only runs for user
                # queries that could not be CTE-wrapped, and that same
                # exotic SQL is what a server-side named cursor is most
                # likely to reject. Maximum compatibility wins here; the
                # memory-optimized streaming stays reserved for the
                # soda-generated DWH transfer queries.
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
