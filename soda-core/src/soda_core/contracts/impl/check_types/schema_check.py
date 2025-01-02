from __future__ import annotations

from dataclasses import dataclass

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_results import QueryResult
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery, MetadataColumn
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.check_types.schema_check_yaml import SchemaCheckYaml
from soda_core.contracts.impl.contract_verification_impl import Metric, Check, MetricsResolver, Query, CheckParser, \
    Contract, Column


class SchemaCheckParser(CheckParser):

    def get_check_type_names(self) -> list[str]:
        return ['schema']

    def parse_check(
        self,
        contract: Contract,
        column: Column | None,
        check_yaml: SchemaCheckYaml,
        metrics_resolver: MetricsResolver,
    ) -> Check | None:
        return SchemaCheck(
            contract=contract,
            check_yaml=check_yaml,
            metrics_resolver=metrics_resolver,
        )


@dataclass
class ExpectedColumn:
    column_name: str
    data_type: str | None


@dataclass
class ColumnDataTypeMismatch:
    column: str
    expected_data_type: str
    actual_data_type: str


class SchemaCheck(Check):

    def __init__(
        self,
        contract: Contract,
        check_yaml: SchemaCheckYaml,
        metrics_resolver: MetricsResolver,
    ):
        super().__init__(
            contract=contract,
            column=None,
            check_yaml=check_yaml,
        )

        self.summary = "schema"

        self.expected_columns: list[ExpectedColumn] = [
            ExpectedColumn(
                column_name=column.column_yaml.name,
                data_type=column.column_yaml.data_type
            )
            for column in contract.columns
        ]

        schema_metric = SchemaMetric(
            contract=contract,
        )
        resolved_schema_metric: SchemaMetric = metrics_resolver.resolve_metric(schema_metric)
        self.metrics["schema"] = resolved_schema_metric

        schema_query: Query = SchemaQuery(
            dataset_prefix=contract.dataset_prefix,
            dataset_name=contract.dataset_name,
            schema_metric=resolved_schema_metric,
            data_source=contract.data_source
        )
        self.queries.append(schema_query)

    def evaluate(self) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        expected_column_names_not_actual: list[str] = []
        actual_column_names_not_expected: list[str] = []
        column_data_type_mismatches: list[ColumnDataTypeMismatch] = []

        actual_columns: list[MetadataColumn] = self.metrics["schema"].value
        if actual_columns:
            actual_column_names = [
                actual_column.column_name for actual_column in actual_columns
            ]
            actual_column_types = {
                actual_column.column_name: actual_column.data_type for actual_column in actual_columns
            }
            expected_column_names = [
                expected_column.column_name for expected_column in self.expected_columns
            ]

            for expected_column in expected_column_names:
                if expected_column not in actual_column_names:
                    expected_column_names_not_actual.append(expected_column)

            for actual_column_name in actual_column_names:
                if actual_column_name not in expected_column_names:
                    actual_column_names_not_expected.append(actual_column_name)

            for expected_column in self.expected_columns:
                expected_data_type: str | None = expected_column.data_type
                actual_data_type: str | None = actual_column_types.get(expected_column.column_name)
                if expected_data_type and actual_data_type != expected_data_type:
                    column_data_type_mismatches.append(
                        ColumnDataTypeMismatch(
                            column=expected_column.column_name,
                            expected_data_type=expected_data_type,
                            actual_data_type=actual_data_type
                        )
                    )

            # TODO add optional index checking
            # schema_column_index_mismatches = {}

            outcome = (
                CheckOutcome.PASSED if (
                    len(expected_column_names_not_actual)
                    + len(actual_column_names_not_expected)
                    + len(column_data_type_mismatches)
                ) == 0
                else CheckOutcome.FAILED
            )

        return SchemaCheckResult(
            expected_columns=self.expected_columns,
            outcome=outcome,
            actual_columns=actual_columns,
            expected_column_names_not_actual=expected_column_names_not_actual,
            actual_column_names_not_expected=actual_column_names_not_expected,
            column_data_type_mismatches=column_data_type_mismatches
        )


class SchemaMetric(Metric):

    def __init__(
        self,
        contract: Contract,
    ):
        super().__init__(
            contract=contract,
            metric_type="schema"
        )


class SchemaQuery(Query):

    def __init__(
        self,
        dataset_prefix: list[str] | None,
        dataset_name: str,
        schema_metric: SchemaMetric,
        data_source: DataSource
    ):
        super().__init__(
            data_source=data_source,
            metrics=[schema_metric]
        )
        self.metadata_columns_query_builder: MetadataColumnsQuery = data_source.create_metadata_columns_query()
        self.sql = self.metadata_columns_query_builder.build_sql(
            dataset_prefix=dataset_prefix,
            dataset_name=dataset_name,
        )

    def execute(self) -> None:
        query_result: QueryResult = self.data_source.execute_query(self.sql)
        metadata_columns: list[MetadataColumn] = self.metadata_columns_query_builder.get_result(query_result)
        self.metrics[0].value = metadata_columns


class SchemaCheckResult(CheckResult):

    def __init__(self,
                 outcome: CheckOutcome,
                 expected_columns: list[ExpectedColumn],
                 actual_columns: list[MetadataColumn],
                 expected_column_names_not_actual: list[str],
                 actual_column_names_not_expected: list[str],
                 column_data_type_mismatches: list[ColumnDataTypeMismatch],
                 ):
        super().__init__(
            outcome=outcome,
            check_summary="Schema",
            diagnostic_lines=self._create_diagnostic_lines(
                expected_columns=expected_columns,
                actual_columns=actual_columns,
                expected_column_names_not_actual=expected_column_names_not_actual,
                actual_column_names_not_expected=actual_column_names_not_expected,
                column_data_type_mismatches=column_data_type_mismatches
            )
        )
        self.expected_columns: list[ExpectedColumn] = expected_columns
        self.actual_columns: list[MetadataColumn] = actual_columns
        self.expected_column_names_not_actual: list[str] = expected_column_names_not_actual
        self.actual_column_names_not_expected: list[str] = actual_column_names_not_expected
        self.column_data_type_mismatches: list[ColumnDataTypeMismatch] = column_data_type_mismatches

    @classmethod
    def _create_diagnostic_lines(
        cls,
        expected_columns: list[ExpectedColumn],
        actual_columns: list[MetadataColumn],
        expected_column_names_not_actual: list[str],
        actual_column_names_not_expected: list[str],
        column_data_type_mismatches: list[ColumnDataTypeMismatch],
    ) -> list[str]:
        def opt_data_type(data_type: str | None) -> str:
            if isinstance(data_type, str):
                return f"({data_type})"
            else:
                return ""

        expected_columns_str: str = ", ".join(
            [
                f"{expected_column.column_name}{opt_data_type(expected_column.data_type)}"
                for expected_column in expected_columns
            ]
        )

        actual_columns_str: str = ", ".join([
            f"{actual_column.column_name}({actual_column.data_type})"
            for actual_column in actual_columns
        ])

        lines: list[str] = [
            f"  Expected schema: {expected_columns_str}",
            f"  Actual schema: {actual_columns_str}",
        ]
        lines.extend(
            [f"  Column '{column}' was present and not allowed" for column in actual_column_names_not_expected]
        )
        lines.extend(
            [f"  Column '{column}' was missing" for column in expected_column_names_not_actual]
        )
        lines.extend(
            [
                (
                    f"  Column '{data_type_mismatch.column}': Expected type '{data_type_mismatch.expected_data_type}', "
                    f"but was '{data_type_mismatch.actual_data_type}'"
                )
                for data_type_mismatch in column_data_type_mismatches
            ]
        )
        return lines

    def dataset_does_not_exists(self) -> bool:
        """
        Helper method for testing
        """
        return len(self.actual_columns) == 0

    def column_does_not_exist(self, column_name: str) -> bool:
        """
        Helper method for testing
        """
        return any(actual_column.column_name == column_name for actual_column in self.actual_columns)