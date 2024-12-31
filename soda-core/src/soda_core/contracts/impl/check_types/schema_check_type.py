from __future__ import annotations

from dataclasses import dataclass

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_results import QueryResult
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery, MetadataColumn
from soda_core.common.yaml import YamlObject
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.contract_verification_impl import Metric, Check, MetricsResolver, Query
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, ContractYaml, CheckType


class SchemaCheckType(CheckType):

    def get_check_type_names(self) -> list[str]:
        return ["schema"]

    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        return SchemaCheckYaml(
            check_yaml_object=check_yaml_object,
        )


class SchemaCheckYaml(CheckYaml):

    def __init__(
        self,
        check_yaml_object: YamlObject,
    ):
        super().__init__(
            check_yaml_object=check_yaml_object,
        )

    def create_check(self, data_source: DataSource, dataset_prefix: list[str] | None, contract_yaml: ContractYaml,
                     column_yaml: ColumnYaml | None, check_yaml: CheckYaml, metrics_resolver: MetricsResolver) -> Check:
        return SchemaCheck(
            contract_yaml=contract_yaml,
            column_yaml=column_yaml,
            check_yaml=self,
            metrics_resolver=metrics_resolver,
            data_source=data_source,
            dataset_prefix=dataset_prefix
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
        contract_yaml: ContractYaml,
        column_yaml: ColumnYaml | None,
        check_yaml: SchemaCheckYaml,
        metrics_resolver: MetricsResolver,
        data_source: DataSource,
        dataset_prefix: list[str] | None
    ):
        super().__init__(
            contract_yaml=contract_yaml,
            column_yaml=column_yaml,
            check_yaml=check_yaml,
            dataset_prefix=dataset_prefix,
            threshold=None,
            summary="Schema"
        )

        self.expected_columns: list[ExpectedColumn] = [
            ExpectedColumn(
                column_name=column_yaml.name,
                data_type=column_yaml.data_type
            )
            for column_yaml in contract_yaml.columns
        ]

        schema_metric = SchemaMetric(
            data_source_name=self.data_source_name,
            dataset_prefix=self.dataset_prefix,
            dataset_name=self.dataset_name,
        )
        resolved_schema_metric: SchemaMetric = metrics_resolver.resolve_metric(schema_metric)
        self.metrics["schema"] = resolved_schema_metric

        schema_query: Query = SchemaQuery(
            dataset_prefix=self.dataset_prefix,
            dataset_name=self.dataset_name,
            schema_metric=resolved_schema_metric,
            data_source=data_source
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
        data_source_name: str,
        dataset_prefix: list[str] | None,
        dataset_name: str | None,
    ):
        super().__init__(
            data_source_name=data_source_name,
            dataset_prefix=dataset_prefix,
            dataset_name=dataset_name,
            column_name=None,
            metric_type_name="schema"
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
