from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome, Measurement, CheckInfo, ContractInfo
from soda_core.contracts.impl.check_types.invalidity_check_yaml import InvalidCheckYaml
from soda_core.contracts.impl.check_types.missing_check_yaml import MissingCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetric
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, Check, AggregationMetric, Threshold, \
    ThresholdType, DerivedPercentageMetric, CheckParser, Contract, Column, MissingAndValidity, MissingAndValidityCheck, \
    Metric, Query, ValidReferenceData, MeasurementValues
from soda_core.contracts.impl.contract_yaml import ColumnYaml, CheckYaml


class InvalidCheckParser(CheckParser):

    def get_check_type_names(self) -> list[str]:
        return ['invalid_count', 'invalid_percent']

    def parse_check(
        self,
        contract: Contract,
        column: Column | None,
        check_yaml: MissingCheckYaml,
        metrics_resolver: MetricsResolver,
    ) -> Check | None:
        return InvalidCheck(
            contract=contract,
            column=column,
            check_yaml=check_yaml,
            metrics_resolver=metrics_resolver,
        )


class InvalidCheck(MissingAndValidityCheck):

    def __init__(
        self,
        contract: Contract,
        column: Column,
        check_yaml: InvalidCheckYaml,
        metrics_resolver: MetricsResolver,
    ):
        super().__init__(
            contract=contract,
            column=column,
            check_yaml=check_yaml,
        )
        self.threshold = Threshold.create(
            check_yaml=check_yaml,
            default_threshold=Threshold(type=ThresholdType.SINGLE_COMPARATOR,must_be=0)
        )
        metric_name: str = Threshold.get_metric_name(check_yaml.type, column=column)
        self.name = (
            self.threshold.get_assertion_summary(metric_name=metric_name) if self.threshold
            else f"{check_yaml.type} (invalid threshold)"
        )

        self.invalid_count_metric: Metric | None = None
        if self.missing_and_validity.has_reference_data():
            # noinspection PyTypeChecker
            self.invalid_count_metric = self._resolve_metric(InvalidReferenceCountMetric(
                contract=contract,
                column=column,
                missing_and_validity=self.missing_and_validity
            ))
            self.queries.append(InvalidReferenceCountQuery(
                metric=self.invalid_count_metric,
                data_source=contract.data_source
            ))
        else:
            self.invalid_count_metric = self._resolve_metric(InvalidCountMetric(
                contract=contract,
                column=column,
                check=self
            ))

        if self.type == "invalid_percent":
            self.row_count_metric = self._resolve_metric(RowCountMetric(
                contract=contract,
            ))

            self.invalid_percent_metric = self._resolve_metric(DerivedPercentageMetric(
                metric_type="invalid_percent",
                fraction_metric=self.invalid_count_metric,
                total_metric=self.row_count_metric
            ))

    def evaluate(self, measurement_values: MeasurementValues, contract_info: ContractInfo) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        invalid_count: int = measurement_values.get_value(self.invalid_count_metric)
        diagnostic_lines = [
            f"Actual invalid_count was {invalid_count}"
        ]

        threshold_value: Number | None = None
        if self.type == "invalid_count":
            threshold_value = invalid_count
        else:
            row_count: int = measurement_values.get_value(self.row_count_metric)
            diagnostic_lines.append(f"Actual row_count was {row_count}")
            if row_count > 0:
                missing_percent: float = measurement_values.get_value(self.invalid_percent_metric)
                diagnostic_lines.append(f"Actual invalid_percent was {missing_percent}")
                threshold_value = missing_percent

        if self.threshold and isinstance(threshold_value, Number):
            if self.threshold.passes(threshold_value):
                outcome = CheckOutcome.PASSED
            else:
                outcome = CheckOutcome.FAILED

        return CheckResult(
            contract=contract_info,
            check=self._build_check_info(),
            metric_value=threshold_value,
            outcome=outcome,
            diagnostic_lines=diagnostic_lines,
        )


class InvalidCountMetric(AggregationMetric):

    def __init__(
        self,
        contract: Contract,
        column: Column,
        check: MissingAndValidityCheck,
    ):
        super().__init__(
            contract=contract,
            column=column,
            metric_type="invalid_count",
        )
        self.missing_and_validity: MissingAndValidity = check.missing_and_validity

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column.column_yaml.name
        return self.missing_and_validity.get_sum_invalid_count_expr(column_name)

    def convert_db_value(self, value) -> any:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        value = 0 if value is None else value
        return int(value)


class InvalidReferenceCountMetric(Metric):

    def __init__(
        self,
        contract: Contract,
        column: Column,
        missing_and_validity: MissingAndValidity
    ):
        super().__init__(
            contract=contract,
            metric_type="invalid_count",
            column=column,
        )
        self.missing_and_validity = missing_and_validity


class InvalidReferenceCountQuery(Query):

    def __init__(
        self,
        metric: InvalidReferenceCountMetric,
        data_source: DataSource
    ):
        super().__init__(
            data_source=data_source,
            metrics=[metric]
        )

        valid_reference_data: ValidReferenceData = metric.missing_and_validity.valid_reference_data

        referencing_dataset_name: str = metric.contract.dataset_name
        referencing_dataset_prefix: str | None = metric.contract.dataset_prefix
        referencing_column_name: str = metric.column.column_yaml.name
        # C stands for the 'C'ontract dataset
        referencing_alias: str = "C"

        referenced_dataset_name: str = valid_reference_data.dataset_name
        referenced_dataset_prefix: list[str] | None = (
            valid_reference_data.dataset_prefix
            if valid_reference_data.dataset_prefix is not None
            else metric.contract.dataset_prefix
        )
        referenced_column: str = valid_reference_data.column
        # R stands for the 'R'eference dataset
        referenced_alias: str = "R"

        # The variant to get the failed rows is:
        # SELECT(STAR().IN("REFERENCING")),
        # which should translate to SELECT REFERENCING.*

        self.sql = self.data_source.sql_dialect.build_select_sql([
            SELECT(COUNT(STAR())),
            FROM(referencing_dataset_name).IN(referencing_dataset_prefix).AS(referencing_alias),
            LEFT_INNER_JOIN(referenced_dataset_name).IN(referenced_dataset_prefix)
                .ON(EQ(COLUMN(referencing_column_name).IN(referencing_alias),
                       COLUMN(referenced_column).IN(referenced_alias)))
                .AS(referenced_alias),
            WHERE(AND([IS_NULL(COLUMN(referenced_column).IN(referenced_alias)),
                       NOT(IS_NULL(COLUMN(referencing_column_name).IN(referencing_alias)))])),
        ])

    def execute(self) -> list[Measurement]:
        query_result: QueryResult = self.data_source.execute_query(self.sql)
        metric_value = query_result.rows[0][0]
        metric: Metric = self.metrics[0]
        return [Measurement(
            metric_id=metric.id,
            value=metric_value,
            metric_name=metric.type
        )]
