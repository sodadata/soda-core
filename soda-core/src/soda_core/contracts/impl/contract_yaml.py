from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from numbers import Number
from typing import Optional

from soda_core.common.logs import Logs, Location, Emoticons
from soda_core.common.yaml import YamlSource, YamlObject, YamlList, YamlValue, YamlFileContent


def register_check_types() -> None:
    from soda_core.contracts.impl.contract_verification_impl import CheckImpl

    from soda_core.contracts.impl.check_types.schema_check_yaml import SchemaCheckYamlParser
    CheckYaml.register(SchemaCheckYamlParser())
    from soda_core.contracts.impl.check_types.schema_check import SchemaCheckParser
    CheckImpl.register(SchemaCheckParser())

    from soda_core.contracts.impl.check_types.missing_check_yaml import MissingCheckYamlParser
    CheckYaml.register(MissingCheckYamlParser())
    from soda_core.contracts.impl.check_types.missing_check import MissingCheckParser
    CheckImpl.register(MissingCheckParser())

    from soda_core.contracts.impl.check_types.invalidity_check_yaml import InvalidCheckYamlParser
    CheckYaml.register(InvalidCheckYamlParser())
    from soda_core.contracts.impl.check_types.invalidity_check import InvalidCheckParser
    CheckImpl.register(InvalidCheckParser())

    from soda_core.contracts.impl.check_types.row_count_check_yaml import RowCountCheckYamlParser
    CheckYaml.register(RowCountCheckYamlParser())
    from soda_core.contracts.impl.check_types.row_count_check import RowCountCheckParser
    CheckImpl.register(RowCountCheckParser())


class ContractYaml:

    """
    Represents YAML as close as possible.
    None means the key was not present.
    If property value types do not match the schema, None value will be in the model
    List properties will have a None value if the property is not present or the content was not a list, a list otherwise
    """

    @classmethod
    def parse(cls, contract_yaml_source: YamlSource, variables: Optional[dict[str, str]] = None,
              logs: Optional[Logs] = None) -> Optional[ContractYaml]:
        logs = logs if logs else Logs()

        check_types_have_been_registered: bool = len(CheckYaml.check_yaml_parsers) > 0
        if not check_types_have_been_registered:
           register_check_types()

        contract_yaml_file_content: Optional[YamlFileContent] = (
            contract_yaml_source.parse_yaml_file_content(file_type="Contract", variables=variables, logs=logs)
        )
        if contract_yaml_file_content and contract_yaml_file_content.has_yaml_object():
            return ContractYaml(contract_yaml_file_content=contract_yaml_file_content)

    def __init__(self, contract_yaml_file_content: YamlFileContent):
        self.logs: Logs = contract_yaml_file_content.logs
        self.contract_yaml_file_content: YamlFileContent = contract_yaml_file_content
        self.contract_yaml_object: Optional[YamlObject] = self.contract_yaml_file_content.get_yaml_object()

        self.data_source: Optional[str] = (
            self.contract_yaml_object.read_string_opt("data_source")
            if self.contract_yaml_object else None
        )
        if self.contract_yaml_object.has_key("datasource") and not self.contract_yaml_object.has_key("data_source"):
            self.logs.error(
                message=(f"{Emoticons.POLICE_CAR_LIGHT} Key `datasource` must be 2 words. "
                         "Please change to `data_source`."),
                location=self.contract_yaml_object.create_location_from_yaml_dict_key("datasource")
            )

        self.dataset_prefix: Optional[list[str]] = (
            self.contract_yaml_object.read_list_of_strings_opt("dataset_prefix")
            if self.contract_yaml_object else None
        )
        self.dataset: Optional[str] = (
            self.contract_yaml_object.read_string("dataset")
            if self.contract_yaml_object else None
        )
        self.columns: Optional[list[Optional[ColumnYaml]]] = self._parse_columns(self.contract_yaml_object)
        self.checks: Optional[list[Optional[CheckYaml]]] = self._parse_checks(self.contract_yaml_object)

    def _parse_columns(self, contract_yaml_object: YamlObject) -> Optional[list[Optional[ColumnYaml]]]:
        columns: Optional[list[Optional[ColumnYaml]]] = None
        if contract_yaml_object:
            column_yaml_objects: Optional[YamlList] = contract_yaml_object.read_list_of_objects_opt("columns")
            if isinstance(column_yaml_objects, YamlList):
                columns = []
                column_locations_by_name: dict[str, list[Optional[Location]]] = {}
                for column_yaml_object in column_yaml_objects:
                    if isinstance(column_yaml_object, YamlObject):
                        column_yaml: ColumnYaml = ColumnYaml(
                            contract_yaml=self,
                            column_yaml_object=column_yaml_object
                        )
                        columns.append(column_yaml)
                        if isinstance(column_yaml.name, str):
                            (column_locations_by_name
                             .setdefault(column_yaml.name, [])
                             .append(column_yaml_object.location))
                for column_name, locations in column_locations_by_name.items():
                    if len(locations) > 1:
                        locations_message: str = ", ".join([
                            f"[{location.line},{location.column}]" for location in locations
                            if location is not None
                        ])
                        file_location = (
                            f" In {self.contract_yaml_file_content.yaml_file_path} at: "
                            if self.contract_yaml_file_content.yaml_file_path
                            else "At file locations: "
                        )
                        locations_message = (
                            f": {file_location}{locations_message}"
                            if locations_message else ""
                        )
                        self.logs.error(
                            f"{Emoticons.POLICE_CAR_LIGHT} Duplicate columns with "
                            f"name '{column_name}'{locations_message}"
                        )
        return columns

    def _parse_checks(
        self,
        checks_containing_yaml_object: YamlObject,
        column_yaml: Optional[ColumnYaml] = None
    ) -> Optional[list[Optional[CheckYaml]]]:
        checks: Optional[list[Optional[CheckYaml]]] = None

        if checks_containing_yaml_object:
            checks_yaml_list: YamlList = checks_containing_yaml_object.read_list_opt("checks")
            if checks_yaml_list:
                checks = []
                for check_yaml_object in checks_yaml_list:
                    check_type_name: Optional[str] = None
                    check_body_yaml_object: Optional[YamlObject] = None

                    if isinstance(check_yaml_object, YamlObject):
                        check_keys: set[str] = check_yaml_object.keys()
                        if len(check_keys) != 1:
                            self.logs.error(
                                message=f"{Emoticons.POLICE_CAR_LIGHT} Checks require 1 key to be the type",
                                location=check_yaml_object.location
                            )
                        else:
                            check_type_name = check_keys.pop()
                            check_body_yaml_object = check_yaml_object.read_object_opt(key=check_type_name)
                    elif isinstance(check_yaml_object, str):
                        check_type_name = check_yaml_object
                        self.logs.error(
                            f"{Emoticons.PINCHED_FINGERS} Mama Mia! You forgot the "
                            f"colon ':' behind the check '{check_type_name}'. For this once I'll "
                            f"still execute it {Emoticons.SEE_NO_EVIL}"
                        )
                    if isinstance(check_type_name, str):
                        if check_body_yaml_object is None:
                            check_body_yaml_object = YamlObject(
                                yaml_file_content=checks_containing_yaml_object.yaml_file_content,
                                yaml_dict={}
                            )
                            check_body_yaml_object.location = checks_containing_yaml_object.location

                        check_yaml: Optional[CheckYaml] = CheckYaml.parse_check_yaml(
                            check_type_name=check_type_name,
                            check_body_yaml_object=check_body_yaml_object,
                            column_yaml=column_yaml,
                            logs=self.logs
                        )
                        if check_yaml:
                            checks.append(check_yaml)
                        else:
                            self.logs.error(
                                f"{Emoticons.POLICE_CAR_LIGHT} Invalid check type '{check_type_name}'. "
                                f"Existing check types: {CheckYaml.get_check_type_names()}"
                            )
                    else:
                        self.logs.error(f"{Emoticons.POLICE_CAR_LIGHT} Checks must have a YAML object structure.")

        return checks


class ValidReferenceDataYaml:

    def __init__(self, valid_reference_data_yaml: YamlObject):
        logs = valid_reference_data_yaml.logs

        dataset: any = valid_reference_data_yaml.read_value("dataset")
        is_list_str: bool = isinstance(dataset, list) and all(isinstance(e, str) for e in dataset)
        self.dataset: str | list[str] | None = dataset if isinstance(dataset, str) or is_list_str else None
        self.column: Optional[str] = valid_reference_data_yaml.read_string("column")

        cfg_keys = valid_reference_data_yaml.yaml_dict.keys()
        self.has_configuration_error: bool = (
            ("dataset" in cfg_keys and self.dataset is None)
            and ("column" in cfg_keys and self.column is None)
        )

        if self.dataset is None:
            self.has_configuration_error = True
            logs.error(
                message=f"{Emoticons.POLICE_CAR_LIGHT} 'dataset' is required. Must be the dataset name as a string "
                        "or a list of strings representing the qualified name.",
                location=valid_reference_data_yaml.location
            )


@dataclass
class RegexFormat:
    regex: str
    name: Optional[str]

    @classmethod
    def read(cls, yaml_object: YamlObject, key: str) -> Optional[RegexFormat]:
        regex_format_yaml_object: Optional[YamlObject] = yaml_object.read_object_opt(key)
        if regex_format_yaml_object:
            regex: Optional[str] = regex_format_yaml_object.read_string("regex")
            name: Optional[str] = regex_format_yaml_object.read_string("name")
            if isinstance(regex, str):
                return RegexFormat(regex=regex, name=name)
        return None


class MissingAndValidityYaml:

    def __init__(self, yaml_object: YamlObject):
        self.missing_values: Optional[list] = YamlValue.yaml_unwrap(yaml_object.read_list_opt("missing_values"))
        self.missing_format: Optional[RegexFormat] = RegexFormat.read(yaml_object=yaml_object, key="missing_format")

        cfg_keys = yaml_object.yaml_dict.keys()
        self.has_missing_configuration_error: bool = (
            ("missing_values" in cfg_keys and self.missing_values is None)
            or ("missing_regex_sql" in cfg_keys and self.missing_regex is None)
        )

        self.invalid_values: Optional[list] = YamlValue.yaml_unwrap(yaml_object.read_list_opt("invalid_values"))
        self.invalid_format: Optional[RegexFormat] = RegexFormat.read(yaml_object=yaml_object, key="invalid_format")
        self.valid_values: Optional[list] = YamlValue.yaml_unwrap(yaml_object.read_list_opt("valid_values"))
        self.valid_format: Optional[RegexFormat] = RegexFormat.read(yaml_object=yaml_object, key="valid_format")
        self.valid_min: Optional[Number] = yaml_object.read_number_opt("valid_min")
        self.valid_max: Optional[Number] = yaml_object.read_number_opt("valid_max")
        self.valid_length: Optional[int] = yaml_object.read_number_opt("valid_length")
        self.valid_min_length: Optional[int] = yaml_object.read_number_opt("valid_min_length")
        self.valid_max_length: Optional[int] = yaml_object.read_number_opt("valid_max_length")

        self.valid_reference_data: Optional[ValidReferenceDataYaml] = None
        valid_reference_data_yaml: Optional[YamlObject] = yaml_object.read_object_opt("valid_reference_data")
        if valid_reference_data_yaml:
            self.valid_reference_data = ValidReferenceDataYaml(valid_reference_data_yaml)
            non_reference_configurations: list[str] = self.get_non_reference_configurations()
            if non_reference_configurations:
                yaml_object.logs.error(
                    f"{Emoticons.POLICE_CAR_LIGHT} 'valid_reference_data' is mutually exclusive with other "
                    f"validity configurations: {non_reference_configurations}"
                )

        self.has_valid_configuration_error: bool = (
            ("invalid_values" in cfg_keys and self.invalid_values is None)
            or ("invalid_format" in cfg_keys and self.invalid_format is None)
            or ("valid_values" in cfg_keys and self.valid_values is None)
            or ("valid_format" in cfg_keys and self.valid_format is None)
            or ("valid_min" in cfg_keys and self.valid_min is None)
            or ("valid_max" in cfg_keys and self.valid_max is None)
            or ("valid_length" in cfg_keys and self.valid_length is None)
            or ("valid_min_length" in cfg_keys and self.valid_min_length is None)
            or ("valid_max_length" in cfg_keys and self.valid_max_length is None)
            or ("valid_reference_data" in cfg_keys and self.valid_reference_data.has_configuration_error)
        )

    def get_non_reference_configurations(self) -> list[str]:
        non_reference_configurations: list[str] = [
            # Combining missing values with reference data validity is allowed!
            # "missing_values" if self.missing_values is not None else None,
            # "missing_regex_sql" if self.missing_regex_sql is not None else None,
            "invalid_values" if self.invalid_values is not None else None,
            "invalid_format" if self.invalid_format is not None else None,
            "valid_values" if self.valid_values is not None else None,
            "valid_format" if self.valid_format is not None else None,
            "valid_min" if self.valid_min is not None else None,
            "valid_max" if self.valid_max is not None else None,
            "valid_length" if self.valid_length is not None else None,
            "valid_min_length" if self.valid_min_length is not None else None,
            "valid_max_length" if self.valid_max_length is not None else None,
        ]
        return [
            cfg for cfg in non_reference_configurations if cfg is not None
        ]


class ColumnYaml(MissingAndValidityYaml):

    def __init__(self, contract_yaml: ContractYaml, column_yaml_object: YamlObject):
        self.column_yaml_object: YamlObject = column_yaml_object
        self.name: Optional[str] = column_yaml_object.read_string("name")
        self.data_type: Optional[str] = column_yaml_object.read_string_opt("data_type")
        self.character_maximum_length: Optional[int] = column_yaml_object.read_number_opt("character_maximum_length")
        super().__init__(column_yaml_object)
        self.check_yamls: Optional[list[CheckYaml]] = contract_yaml._parse_checks(
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
    def read_opt(cls, check_yaml_object: YamlObject, key: str) -> Optional[RangeYaml]:
        range_yaml_list: YamlList = check_yaml_object.read_list_opt(key)
        if range_yaml_list:
            lower_bound: Optional[Number] = None
            upper_bound: Optional[Number] = None
            range_list: list = range_yaml_list.to_list()
            if len(range_list) > 0 and isinstance(range_list[0], Number):
                lower_bound = range_list[0]
            if len(range_list) > 1 and isinstance(range_list[1], Number):
                upper_bound = range_list[1]
            return RangeYaml(lower_bound=lower_bound, upper_bound=upper_bound)


class CheckYamlParser(ABC):

    @abstractmethod
    def get_check_type_names(self) -> list[str]:
        pass

    @abstractmethod
    def parse_check_yaml(
        self,
        check_type_name: str,
        check_yaml_object: YamlObject,
        column_yaml: Optional[ColumnYaml],
        logs: Logs
    ) -> Optional[CheckYaml]:
        pass


class CheckYaml(ABC):

    check_yaml_parsers: dict[str, CheckYamlParser] = {}

    @classmethod
    def register(cls, check_yaml_parser: CheckYamlParser) -> None:
        for check_type_name in check_yaml_parser.get_check_type_names():
            cls.check_yaml_parsers[check_type_name] = check_yaml_parser

    @classmethod
    def get_check_type_names(cls) -> list[str]:
        return list(cls.check_yaml_parsers.keys())

    @classmethod
    def parse_check_yaml(
        cls,
        check_type_name: str,
        check_body_yaml_object: YamlObject,
        column_yaml: Optional[ColumnYaml],
        logs: Logs
    ) -> Optional[CheckYaml]:
        if isinstance(check_type_name, str):
            check_yaml_parser: Optional[CheckYamlParser] = cls.check_yaml_parsers.get(check_type_name)
            if check_yaml_parser:
                return check_yaml_parser.parse_check_yaml(
                    check_type_name=check_type_name,
                    check_yaml_object=check_body_yaml_object,
                    column_yaml=column_yaml,
                    logs=logs
                )

    def __init__(self, type_name: str, check_yaml_object: YamlObject, logs: Logs):
        self.check_yaml_object: YamlObject = check_yaml_object
        self.logs: Logs = logs
        self.type_name: str = type_name
        self.name: Optional[str] = check_yaml_object.read_string_opt("name") if check_yaml_object else None
        qualifier = check_yaml_object.read_value("qualifier") if check_yaml_object else None
        self.qualifier: Optional[str] = str(qualifier) if qualifier is not None else None


class ThresholdCheckYaml(CheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject, logs: Logs):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object, logs=logs)
        self.metric: Optional[str] = check_yaml_object.read_string_opt("metric")
        self.threshold: Optional[ThresholdYaml] = None
        threshold_yaml_object: YamlObject = check_yaml_object.read_object_opt("threshold")
        if threshold_yaml_object:
            self.threshold = ThresholdYaml(threshold_yaml_object)


class ThresholdYaml:

    def __init__(self, threshold_yaml_object: YamlObject):
        self.must_be_greater_than: Optional[Number] = threshold_yaml_object.read_number_opt("must_be_greater_than")
        self.must_be_greater_than_or_equal: Optional[Number] = threshold_yaml_object.read_number_opt("must_be_greater_than_or_equal")
        self.must_be_less_than: Optional[Number] = threshold_yaml_object.read_number_opt("must_be_less_than")
        self.must_be_less_than_or_equal: Optional[Number] = threshold_yaml_object.read_number_opt("must_be_less_than_or_equal")
        self.must_be: Optional[Number] = threshold_yaml_object.read_number_opt("must_be")
        self.must_not_be: Optional[Number] = threshold_yaml_object.read_number_opt("must_not_be")
        self.must_be_between: Optional[RangeYaml] = RangeYaml.read_opt(threshold_yaml_object, "must_be_between")
        self.must_be_not_between: Optional[RangeYaml] = RangeYaml.read_opt(threshold_yaml_object, "must_be_not_between")


class MissingAncValidityCheckYaml(ThresholdCheckYaml, MissingAndValidityYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject, logs: Logs):
        ThresholdCheckYaml.__init__(self, type_name=type_name, check_yaml_object=check_yaml_object, logs=logs)
        MissingAndValidityYaml.__init__(self, yaml_object=check_yaml_object)
