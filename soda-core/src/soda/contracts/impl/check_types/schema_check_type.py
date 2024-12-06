from __future__ import annotations

from dataclasses import dataclass

from soda.common.logs import Logs
from soda.common.yaml import YamlObject
from soda.contracts.contract_verification import CheckResult, CheckOutcome
from soda.contracts.impl.contract_parser import CheckType
from soda.contracts.impl.contract_yaml import CheckYaml


class SchemaCheckType(CheckType):

    def get_check_type_name(self) -> str:
        return "schema"

    def parse_check_yaml(self, check_yaml_object: YamlObject, logs: Logs) -> CheckYaml | None:
        return SchemaCheckYaml(check_yaml_object)


class SchemaCheckYaml(CheckYaml):

    def __init__(self, check_yaml_object: YamlObject):
        super().__init__()
        self.check_yaml_object = check_yaml_object


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
