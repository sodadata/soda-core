from __future__ import annotations

from abc import ABC, abstractmethod
from numbers import Number

from soda_core.common.data_source import DataSource
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource, YamlObject, YamlList, YamlValue


class CheckType(ABC):

    __CHECK_TYPES_BY_NAME: dict[str, CheckType] = {}

    @classmethod
    def register_check_type(cls, check_type: CheckType):
        cls.__CHECK_TYPES_BY_NAME[check_type.name] = check_type

    @classmethod
    def get_check_type(cls, check_type_name: str) -> CheckType | None:
        return cls.__CHECK_TYPES_BY_NAME.get(check_type_name)

    @classmethod
    def get_all_type_names(cls):
        return cls.__CHECK_TYPES_BY_NAME.keys()

    def __init__(self, name: str):
        self.name: str = name

    @abstractmethod
    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        pass


class ContractYaml:

    """
    Represents YAML as close as possible.
    None means the key was not present.
    If property value types do not match the schema, None value will be in the model
    List properties will have a None value if the property is not present or the content was not a list, a list otherwise
    """

    def __init__(self, contract_yaml_file: YamlSource):
        assert contract_yaml_file.is_parsed
        assert isinstance(contract_yaml_file, YamlSource)
        self.contract_yaml_file: YamlSource = contract_yaml_file
        self.logs: Logs = contract_yaml_file.logs

        contract_yaml_object = self.contract_yaml_file.get_yaml_object()
        self.contract_yaml_object: YamlObject = contract_yaml_object

        self.data_source_file: str | None = (
            contract_yaml_object.read_string_opt("data_source_file")
            if contract_yaml_object else None)

        self.data_source_path: list[str] | None = (
            contract_yaml_object.read_list_of_strings("data_source_path")
            if contract_yaml_object else None)

        self.dataset_name: str | None = (
            contract_yaml_object.read_string("dataset")
            if contract_yaml_object else None)

        self.columns: list[ColumnYaml | None] = self._parse_columns(contract_yaml_object)
        self.checks: list[CheckYaml | None] = self._parse_checks(contract_yaml_object)

    def _parse_columns(self, contract_yaml_object: YamlObject) -> list[ColumnYaml] | None:
        columns: list[ColumnYaml] = []
        if contract_yaml_object:
            column_yaml_objects: YamlList | None = contract_yaml_object.read_list_of_objects("columns")
            if isinstance(column_yaml_objects, YamlList):
                for column_yaml_object in column_yaml_objects:
                    column: ColumnYaml | None = None
                    if isinstance(column_yaml_object, YamlObject):
                        column = ColumnYaml(self, column_yaml_object)
                    columns.append(column)
        return columns

    def _parse_checks(
        self,
        checks_containing_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None = None
    ) -> list[CheckYaml | None]:
        checks: list[CheckYaml | None] = []

        if checks_containing_yaml_object:
            checks_yaml_list: YamlList = checks_containing_yaml_object.read_list_opt("checks")
            if checks_yaml_list:
                for check_yaml_object in checks_yaml_list:
                    check_yaml: CheckYaml | None = None
                    if isinstance(check_yaml_object, YamlObject):
                        check_type_name: str | None = check_yaml_object.read_string("type")

                        from soda_core.contracts.impl.check_types.schema_check_type import SchemaCheckType
                        CheckType.register_check_type(SchemaCheckType())
                        from soda_core.contracts.impl.check_types.mising_check_type import MissingCheckType
                        CheckType.register_check_type(MissingCheckType())

                        check_type: CheckType = CheckType.get_check_type(check_type_name)
                        if check_type:
                            check_yaml = check_type.parse_check_yaml(
                                check_yaml_object=check_yaml_object,
                                column_yaml=column_yaml
                            )
                        else:
                            self.logs.error(
                                f"Invalid check type '{check_type_name}'. "
                                f"Existing check types: {CheckType.get_all_type_names()}"
                            )
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

        self.checks: list[CheckYaml] | None = contract_yaml._parse_checks(
            checks_containing_yaml_object=column_yaml_object,
            column_yaml=self
        )


class RangeYaml:
    """
    Boundary values are inclusive
    """

    def __init__(self, lower_bound: Number, upper_bound: Number):
        self.lower_bound: Number = lower_bound
        self.upper_bound: Number= upper_bound

    @classmethod
    def read_opt(cls, check_yaml_object: YamlObject, key: str) -> RangeYaml | None:
        range_yaml_list: YamlList = check_yaml_object.read_list_opt(key)
        if range_yaml_list:
            lower_bound: Number | None = None
            upper_bound: Number | None = None
            range_list: list = range_yaml_list.to_list()
            if len(range_list) > 0 and isinstance(range_list[0], Number):
                lower_bound = range_list[0]
            if len(range_list) > 1 and isinstance(range_list[1], Number):
                upper_bound = range_list[1]
            return RangeYaml(lower_bound=lower_bound, upper_bound=upper_bound)


class CheckYaml(ABC):

    def __init__(self, check_yaml_object: YamlObject):
        self.check_yaml_object: YamlObject = check_yaml_object

        self.type: str = check_yaml_object.read_string("type")
        self.name: str | None = check_yaml_object.read_string_opt("name")
        self.qualifier: str | None = check_yaml_object.read_string_opt("qualifier")

        self.must_be_greater_than: Number | None = check_yaml_object.read_number_opt("must_be_greater_than")
        self.must_be_greater_than_or_equal: Number | None = check_yaml_object.read_number_opt("must_be_greater_than_or_equal")
        self.must_be_less_than: Number | None = check_yaml_object.read_number_opt("must_be_less_than")
        self.must_be_less_than_or_equal: Number | None = check_yaml_object.read_number_opt("must_be_less_than_or_equal")
        self.must_be: Number | None = check_yaml_object.read_number_opt("must_be")
        self.must_not_be: Number | None = check_yaml_object.read_number_opt("must_not_be")
        self.must_be_between: RangeYaml | None = RangeYaml.read_opt(check_yaml_object, "must_be_between")
        self.must_be_not_between: RangeYaml | None = RangeYaml.read_opt(check_yaml_object, "must_be_not_between")

    @abstractmethod
    def create_check(
        self,
        check_yaml: CheckYaml,
        column_yaml: ColumnYaml | None,
        contract_yaml: ContractYaml,
        metrics_resolver: 'MetricsResolver',
        data_source: DataSource
    ) -> 'Check':
        pass
