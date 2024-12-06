from __future__ import annotations

from abc import ABC, abstractmethod

from soda.common.logs import Logs
from soda.common.yaml import YamlFile, YamlObject, YamlList
from soda.contracts.impl.check_types.check_type import CheckType
from soda.contracts.impl.contract_yaml import ContractYaml, CheckYaml, ColumnYaml, ValidValuesReferenceDataYaml


class CheckTypes:

    def __init__(self):
        self.check_type_by_name: dict[str, CheckType] = self.__create_check_types_by_name(
            user_defined_check_factories=None
        )

    def get(self, check_type: str) -> CheckType:
        return self.check_type_by_name.get(check_type)

    def __create_check_types_by_name(
            cls,
            user_defined_check_factories: list[CheckType] | None
    ) -> dict[str, CheckType]:
        all_check_types: list[CheckType] = cls.__create_default_check_types()
        if isinstance(user_defined_check_factories, list):
            all_check_types.extend(user_defined_check_factories)
        return {
            check_type.get_check_type_name(): check_type
            for check_type in all_check_types
        }

    def __create_default_check_types(cls) -> list[CheckType]:
        from soda.contracts.impl.check_types.schema_check_type import SchemaCheckType
        from soda.contracts.impl.check_types.mising_check_type import MissingCheckType
        return [
            SchemaCheckType(),
            MissingCheckType()
        ]


check_types: CheckTypes = CheckTypes()


class ContractParser:

    def __init__(
            self,
            contract_yaml_file: YamlFile
    ):
        self.contract_yaml_file: YamlFile = contract_yaml_file
        self.logs: Logs = contract_yaml_file.logs

    def parse(self, variables: dict[str, str] | None = None) -> ContractYaml | None:
        self.contract_yaml_file.parse(variables)

        contract_yaml: ContractYaml = ContractYaml()
        contract_yaml.contract_yaml_file = self.contract_yaml_file

        contract_yaml_object = self.contract_yaml_file.get_yaml_object()
        if not isinstance(contract_yaml_object, YamlObject):
            return None

        contract_yaml.data_source_name = contract_yaml_object.read_string_opt("data_source")
        contract_yaml.database_name = contract_yaml_object.read_string_opt("database")
        contract_yaml.schema_name = contract_yaml_object.read_string_opt("schema")
        contract_yaml.dataset_name = contract_yaml_object.read_string("dataset")
        contract_yaml.columns = self._parse_columns(contract_yaml_object)

        contract_checks_yaml: YamlList | None = contract_yaml_object.read_list_opt("checks")
        if contract_checks_yaml:
            contract_yaml.checks = self._parse_contract_checks(contract_checks_yaml)

        return contract_yaml

    def _parse_columns(self, contract_yaml_object: YamlObject) -> list[ColumnYaml] | None:
        columns: list[ColumnYaml] = []
        column_yaml_objects: YamlList | None = contract_yaml_object.read_list_of_objects("columns")
        if isinstance(column_yaml_objects, YamlList):
            for column_yaml_object in column_yaml_objects:
                column: ColumnYaml | None = None
                if isinstance(column_yaml_object, YamlObject):
                    column = self._parse_column(column_yaml_object)
                columns.append(column)
        return columns

    def _parse_column(self, column_yaml_object: YamlObject) -> ColumnYaml:
        column: ColumnYaml = ColumnYaml()
        column.column_yaml = column_yaml_object
        column.name = column_yaml_object.read_string("name")
        column.data_type = column_yaml_object.read_string_opt("data_type")

        column.missing_values = column_yaml_object.read_list_opt("missing_values")
        column.missing_regex_sql = column_yaml_object.read_string_opt("missing_regex_sql")
        column.invalid_values = column_yaml_object.read_list_opt("invalid_values")
        column.invalid_format = column_yaml_object.read_string_opt("invalid_format")
        column.invalid_regex_sql = column_yaml_object.read_string_opt("invalid_regex_sql")
        column.valid_values = column_yaml_object.read_list_opt("valid_values")
        column.valid_format = column_yaml_object.read_string_opt("valid_format")
        column.valid_regex_sql = column_yaml_object.read_string_opt("valid_regex_sql")
        column.valid_min = column_yaml_object.read_number_opt("valid_min")
        column.valid_max = column_yaml_object.read_number_opt("valid_max")
        column.valid_length = column_yaml_object.read_number_opt("valid_length")
        column.valid_min_length = column_yaml_object.read_number_opt("valid_min_length")
        column.valid_max_length = column_yaml_object.read_number_opt("valid_max_length")
        column.valid_values_reference_data = None
        ref_data_yaml: YamlObject | None = column_yaml_object.read_object_opt(
            "valid_values_reference_data"
        )
        if ref_data_yaml:
            ref_data = ValidValuesReferenceDataYaml()
            ref_data.ref_dataset = ref_data_yaml.read_string("dataset")
            ref_data.ref_column = ref_data_yaml.read_string("column")
            column.valid_values_reference_data = ref_data

        column_checks_yaml: YamlList | None = column_yaml_object.read_list_opt("checks")
        if column_checks_yaml:
            column.checks = self._parse_column_checks(column_checks_yaml)

        return column

    def _parse_column_checks(self, column_checks_yaml: YamlList) -> list[CheckYaml | None]:
        column_checks: list[CheckYaml | None] = []
        for column_check_yaml in column_checks_yaml:
            column_check: CheckYaml | None = None
            if isinstance(column_check_yaml, YamlObject):
                column_check = self._parse_check(column_check_yaml)
            else:
                self.logs.error(f"Checks must have a YAML object structure.")
            column_checks.append(column_check)
        return column_checks

    def _parse_contract_checks(self, contract_checks_yaml: YamlList) -> list[CheckYaml | None]:
        contract_checks: list[CheckYaml | None] = []
        for check_yaml_object in contract_checks_yaml:
            check_yaml: CheckYaml | None = None
            if isinstance(check_yaml_object, YamlObject):
                check_yaml = self._parse_check(check_yaml_object)
            else:
                self.logs.error(f"Checks must have a YAML object structure.")
            contract_checks.append(check_yaml)
        return contract_checks

    def _parse_check(self, check_yaml_object: YamlObject) -> CheckYaml | None:
        check_type_name: str | None = check_yaml_object.read_string("type")
        check_type: CheckType = check_types.get(check_type_name)
        if check_type:
            return check_type.parse_check_yaml(check_yaml_object=check_yaml_object, logs=self.logs)
        else:
            self.logs.error(f"Invalid check type '{check_type_name}'")
