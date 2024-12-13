from __future__ import annotations

from abc import ABC, abstractmethod
from numbers import Number

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlFile, YamlObject, YamlList, YamlValue
from soda_core.contracts.impl.contract_verification_impl import Check


class CheckType(ABC):

    @abstractmethod
    def get_check_type_name(self) -> str:
        pass

    @abstractmethod
    def parse_check_yaml(self, check_yaml_object: YamlObject, logs: Logs) -> CheckYaml | None:
        pass

    @abstractmethod
    def create_check(
        self,
        contract_yaml: ContractYaml,
        check_yaml: CheckYaml,
        column_yaml: ColumnYaml | None,
    ) -> Check:
        pass


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
        from soda_core.contracts.impl.check_types.schema_check_type import SchemaCheckType
        from soda_core.contracts.impl.check_types.mising_check_type import MissingCheckType
        return [
            SchemaCheckType(),
            MissingCheckType()
        ]


check_types: CheckTypes = CheckTypes()


class ContractYaml:

    """
    Represents YAML as close as possible.
    None means the key was not present.
    If property value types do not match the schema, None value will be in the model
    List properties will have a None value if the property is not present or the content was not a list, a list otherwise
    """

    def __init__(self, contract_yaml_file: YamlFile):
        assert contract_yaml_file.is_parsed
        assert isinstance(contract_yaml_file, YamlFile)
        self.contract_yaml_file: YamlFile = contract_yaml_file
        self.logs: Logs = contract_yaml_file.logs

        contract_yaml_object = self.contract_yaml_file.get_yaml_object()
        assert isinstance(contract_yaml_object, YamlObject)
        self.contract_yaml_object: YamlObject = contract_yaml_object

        self.data_source_name: str | None = contract_yaml_object.read_string_opt("data_source")
        self.database_name: str | None = contract_yaml_object.read_string_opt("database")
        self.schema_name: str | None = contract_yaml_object.read_string_opt("schema")
        self.dataset_name: str | None = contract_yaml_object.read_string("dataset")
        self.columns: list[ColumnYaml | None] = self._parse_columns(contract_yaml_object)
        self.checks: list[CheckYaml | None] = self._parse_checks(contract_yaml_object)

    def _parse_columns(self, contract_yaml_object: YamlObject) -> list[ColumnYaml] | None:
        columns: list[ColumnYaml] = []
        column_yaml_objects: YamlList | None = contract_yaml_object.read_list_of_objects("columns")
        if isinstance(column_yaml_objects, YamlList):
            for column_yaml_object in column_yaml_objects:
                column: ColumnYaml | None = None
                if isinstance(column_yaml_object, YamlObject):
                    column = ColumnYaml(self, column_yaml_object)
                columns.append(column)
        return columns

    def _parse_checks(self, contract_yaml_object: YamlObject, column_yaml: ColumnYaml | None = None) -> list[CheckYaml | None]:
        checks: list[CheckYaml | None] = []
        for check_yaml_object in checks_yaml_list:
            check_yaml: CheckYaml | None = None
            if isinstance(check_yaml_object, YamlObject):
                check_type_name: str | None = check_yaml_object.read_string("type")
                check_type: CheckType = check_types.get(check_type_name)
                if check_type:
                    check_yaml = check_type.parse_check_yaml(check_yaml_object=check_yaml_object, logs=self.logs)
                    if check_yaml:
                        # Default check properties for each check type are parsed here
                        check_yaml.check_yaml_object = check_yaml_object
                        check_yaml.type = check_type_name
                        check_yaml.name = check_yaml_object.read_string_opt("name")
                        check_yaml.qualifier = check_yaml_object.read_string_opt("qualifier")
                else:
                    self.logs.error(f"Invalid check type '{check_type_name}'")
            else:
                self.logs.error(f"Checks must have a YAML object structure.")
            checks.append(check_yaml)
        return checks


class ValidValuesReferenceDataYaml:

    def __init__(self):
        self.ref_dataset: str | None = None
        self.ref_column: str | None = None


class ColumnYaml:

    def __init__(self, contract_yaml: ContractYaml, column_yaml_object: YamlObject):
        self.column_yaml_object: YamlObject = column_yaml_object
        self.name: str | None = column_yaml_object.read_string("name")
        self.data_type: str | None = column_yaml_object.read_string_opt("data_type")
        self.missing_values: list | None = YamlValue.yaml_unwrap(column_yaml_object.read_list_opt("missing_values"))
        self.missing_regex_sql: str | None = column_yaml_object.read_string_opt("missing_regex_sql")

        self.invalid_values: list | None = column_yaml_object.read_list_opt("invalid_values")
        self.invalid_format: str | None = column_yaml_object.read_string_opt("invalid_format")
        self.invalid_regex_sql: str | None = column_yaml_object.read_string_opt("invalid_regex_sql")
        self.valid_values: list | None = column_yaml_object.read_list_opt("valid_values")
        self.valid_format: str | None = column_yaml_object.read_string_opt("valid_format")
        self.valid_regex_sql: str | None = column_yaml_object.read_string_opt("valid_regex_sql")
        self.valid_min: Number | None = column_yaml_object.read_number_opt("valid_min")
        self.valid_max: Number | None = column_yaml_object.read_number_opt("valid_max")
        self.valid_length: int | None = column_yaml_object.read_number_opt("valid_length")
        self.valid_min_length: int | None = column_yaml_object.read_number_opt("valid_min_length")
        self.valid_max_length: int | None = column_yaml_object.read_number_opt("valid_max_length")

        ref_data_yaml: YamlObject | None = column_yaml_object.read_object_opt(
            "valid_values_reference_data"
        )
        self.valid_values_reference_data: ValidValuesReferenceDataYaml | None = None
        if ref_data_yaml:
            self.valid_values_reference_data = ValidValuesReferenceDataYaml()
            self.valid_values_reference_data.ref_dataset = ref_data_yaml.read_string("dataset")
            self.valid_values_reference_data.ref_column = ref_data_yaml.read_string("column")

        self.checks: list[CheckYaml] | None = None
        column_checks_yaml_list: YamlList | None = column_yaml_object.read_list_opt("checks")
        if column_checks_yaml_list:
            self.checks = contract_yaml._parse_checks(
                checks_yaml_list=column_checks_yaml_list,
                column_yaml=self
            )


class RangeYaml:
    """
    Boundary values are inclusive
    """

    def __init__(self):
        self.lower_bound: Number | None
        self.upper_bound: Number | None


class CheckYaml:

    def __init__(self):
        self.check_yaml_object: YamlObject | None = None

        self.type: str | None = None
        self.name: str | None = None
        self.qualifier: str | None = None

        self.must_be_greater_than: Number | None = None
        self.must_be_greater_than_or_equal: Number | None = None
        self.must_be_less_than: Number | None = None
        self.must_be_less_than_or_equal: Number | None = None
        self.must_be: Number | None = None
        self.must_not_be: Number | None = None
        self.must_be_between: RangeYaml | None = None
        self.must_be_not_between: RangeYaml | None = None
