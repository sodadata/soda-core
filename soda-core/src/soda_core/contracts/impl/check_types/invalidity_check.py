from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome, Measurement, Check, Contract, \
    Diagnostic, NumericDiagnostic
from soda_core.contracts.impl.check_types.invalidity_check_yaml import InvalidCheckYaml
from soda_core.contracts.impl.check_types.missing_check_yaml import MissingCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetric
from soda_core.contracts.impl.contract_verification_impl import MetricsResolver, CheckImpl, AggregationMetricImpl, ThresholdImpl, \
    ThresholdType, DerivedPercentageMetricImpl, CheckParser, ContractImpl, ColumnImpl, MissingAndValidity, MissingAndValidityCheckImpl, \
    MetricImpl, Query, ValidReferenceData, MeasurementValues
from soda_core.contracts.impl.contract_yaml import ColumnYaml, CheckYaml


class InvalidCheckParser(CheckParser):

    def get_check_type_names(self) -> list[str]:
        return ['invalid_count', 'invalid_percent']

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl | None,
        check_yaml: MissingCheckYaml,
    ) -> CheckImpl | None:
        return InvalidCheck(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class InvalidCheck(MissingAndValidityCheckImpl):

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
            check_yaml=check_yaml,
            default_threshold=ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be=0)
        )

        # TODO create better support in class hierarchy for common vs specific stuff.  name is common.  see other check type impls
        metric_name: str = ThresholdImpl.get_metric_name(check_yaml.type_name, column_impl=column_impl)
        self.name = check_yaml.name if check_yaml.name else (
            self.threshold.get_assertion_summary(metric_name=metric_name) if self.threshold
            else f"{check_yaml.type_name} (invalid threshold)"
        )

        self.invalid_count_metric_impl: MetricImpl | None = None
        if self.missing_and_validity.has_reference_data():
            # noinspection PyTypeChecker
            self.invalid_count_metric_impl = self._resolve_metric(InvalidReferenceCountMetricImpl(
                contract_impl=contract_impl,
                column_impl=column_impl,
                missing_and_validity=self.missing_and_validity
            ))
            self.queries.append(InvalidReferenceCountQuery(
                metric_impl=self.invalid_count_metric_impl,
                data_source=contract_impl.data_source
            ))
        else:
            self.invalid_count_metric_impl = self._resolve_metric(InvalidCountMetric(
                contract_impl=contract_impl,
                column_impl=column_impl,
                check_impl=self
            ))

        if self.type == "invalid_percent":
            self.row_count_metric = self._resolve_metric(RowCountMetric(
                contract_impl=contract_impl,
            ))

            self.invalid_percent_metric = self._resolve_metric(DerivedPercentageMetricImpl(
                metric_type="invalid_percent",
                fraction_metric_impl=self.invalid_count_metric_impl,
                total_metric_impl=self.row_count_metric
            ))

    def evaluate(self, measurement_values: MeasurementValues, contract_info: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        invalid_count: int = measurement_values.get_value(self.invalid_count_metric_impl)
        diagnostics: list[Diagnostic] = [
            NumericDiagnostic(name="invalid_count", value=invalid_count)
        ]

        threshold_value: Number | None = None
        if self.type == "invalid_count":
            threshold_value = invalid_count
        else:
            row_count: int = measurement_values.get_value(self.row_count_metric)
            diagnostics.append(NumericDiagnostic(name="row_count", value=row_count))
            if row_count > 0:
                invalid_percent: float = measurement_values.get_value(self.invalid_percent_metric)
                diagnostics.append(NumericDiagnostic(name="invalid_percent", value=invalid_percent))
                threshold_value = invalid_percent

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
            diagnostics=diagnostics
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
        )
        self.missing_and_validity: MissingAndValidity = check_impl.missing_and_validity

    def sql_expression(self) -> SqlExpression:
        column_name: str = self.column_impl.column_yaml.name
        return self.missing_and_validity.get_sum_invalid_count_expr(column_name)

    def convert_db_value(self, value) -> any:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        value = 0 if value is None else value
        return int(value)


class InvalidReferenceCountMetricImpl(MetricImpl):

    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        missing_and_validity: MissingAndValidity
    ):
        super().__init__(
            contract_impl=contract_impl,
            metric_type="invalid_count",
            column_impl=column_impl,
        )
        self.missing_and_validity = missing_and_validity


class InvalidReferenceCountQuery(Query):

    def __init__(
        self,
        metric_impl: InvalidReferenceCountMetricImpl,
        data_source: DataSource
    ):
        super().__init__(
            data_source=data_source,
            metrics=[metric_impl]
        )

        valid_reference_data: ValidReferenceData = metric_impl.missing_and_validity.valid_reference_data

        referencing_dataset_name: str = metric_impl.contract_impl.dataset_name
        referencing_dataset_prefix: str | None = metric_impl.contract_impl.dataset_prefix
        referencing_column_name: str = metric_impl.column_impl.column_yaml.name
        # C stands for the 'C'ontract dataset
        referencing_alias: str = "C"

        referenced_dataset_name: str = valid_reference_data.dataset_name
        referenced_dataset_prefix: list[str] | None = (
            valid_reference_data.dataset_prefix
            if valid_reference_data.dataset_prefix is not None
            else metric_impl.contract_impl.dataset_prefix
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
        metric_impl: MetricImpl = self.metrics[0]
        return [Measurement(
            metric_id=metric_impl.id,
            value=metric_value,
            metric_name=metric_impl.type
        )]
