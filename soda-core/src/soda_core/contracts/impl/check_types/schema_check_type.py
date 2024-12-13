from __future__ import annotations

from dataclasses import dataclass

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import QueryResult
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlObject
from soda_core.contracts.contract_verification import CheckResult, CheckOutcome
from soda_core.contracts.impl.contract_verification_impl import Metric, CheckType, Query, Check, FetchAllQueryMetric, \
    Queries, QueryBuilder
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, ContractYaml


class SchemaCheckType(CheckType):

    def get_check_type_name(self) -> str:
        return "schema"

    def parse_check_yaml(self, check_yaml_object: YamlObject, logs: Logs) -> CheckYaml | None:
        return SchemaCheckYaml()

    def create_check_result(self, measurements: dict[Metric, object]) -> CheckResult:
        pass


class SchemaCheckYaml(CheckYaml):

    def __init__(self):
        super().__init__()

    def create_check(self) -> Check:
        return SchemaCheck()


class SchemaCheck(Check):

    def __init__(self, contract_yaml: ContractYaml, check_yaml: CheckYaml, column_yaml: ColumnYaml | None):
        super().__init__(contract_yaml, check_yaml, column_yaml)

    def _create_metrics(self) -> list[Metric]:
        return [
            SchemaMetric(
                data_source_name=self.data_source_name,
                database_name=self.database_name,
                schema_name=self.schema_name,
                dataset_name=self.dataset_name,
                column_name=self.column_name,
            )
        ]

    def evaluate(self) -> CheckResult:
        pass


class SchemaMetric(FetchAllQueryMetric):

    def __init__(
        self,
        data_source_name: str,
        database_name: str | None,
        schema_name: str | None,
        dataset_name: str,
        column_name: str | None,
    ):
        super().__init__("schema")

    def add_to_queries(self, queries: Queries):
        queries.add_query()

    def get_query_sql(self) -> str:
        pass

    def create_measurement_value(self, query_result: QueryResult) -> object:
        pass


class SchemaQueryBuilder(QueryBuilder):

    def __init__(self, database_name: str | None, schema_name: str | None, dataset_name: str):
        super().__init__()
        self.database_name: str | None = database_name
        self.schema_name: str | None = schema_name
        self.dataset_name: str = dataset_name




@dataclass
class DataTypeMismatch:
    column: str
    expected_data_type: str
    actual_data_type: str


class SchemaCheckResult(CheckResult):

    def __init__(self,
                 check_yaml: SchemaCheckYaml,
                 outcome: CheckOutcome,
                 measured_schema: dict[str, str],
                 columns_not_allowed_and_present: list[str] | None,
                 columns_required_and_not_present: list[str] | None,
                 columns_having_wrong_type: list[DataTypeMismatch] | None,
                 ):
        super().__init__(check_yaml, outcome)
        self.measured_schema: dict[str, str] = measured_schema
        self.columns_not_allowed_and_present: list[str] | None = columns_not_allowed_and_present
        self.columns_required_and_not_present: list[str] | None = columns_required_and_not_present
        self.columns_having_wrong_type: list[DataTypeMismatch] | None = columns_having_wrong_type

    def dataset_does_not_exists(self) -> bool:
        return isinstance(self.measured_schema, dict) and len(self.measured_schema) == 0

    def column_does_not_exist(self, column_name: str) -> bool:
        return column_name not in self.measured_schema

    def get_contract_result_str_lines(self) -> list[str]:
        # noinspection PyTypeChecker
        schema_check_yaml: SchemaCheckYaml = self.check_yaml
        expected_schema: str = ",".join(
            [
                f"{c.get('name')}{c.get('optional')}{c.get('type')}"
                for c in [
                    {
                        "name": column_name,
                        "optional": "(optional)" if column_name in schema_check_yaml.optional_columns else "",
                        "type": f"={data_type}" if data_type else "",
                    }
                    for column_name, data_type in schema_check_yaml.columns.items()
                ]
            ]
        )

        lines: list[str] = [
            f"Schema check {self.get_outcome_str()}",
            f"  Expected schema: {expected_schema}",
            f"  Actual schema: {self.measured_schema}",
        ]
        lines.extend(
            [f"  Column '{column}' was present and not allowed" for column in self.columns_not_allowed_and_present]
        )
        lines.extend([f"  Column '{column}' was missing" for column in self.columns_required_and_not_present])
        lines.extend(
            [
                (
                    f"  Column '{data_type_mismatch.column}': Expected type '{data_type_mismatch.expected_data_type}', "
                    f"but was '{data_type_mismatch.actual_data_type}'"
                )
                for data_type_mismatch in self.columns_having_wrong_type
            ]
        )
        return lines
