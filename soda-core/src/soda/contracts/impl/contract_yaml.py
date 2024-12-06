from __future__ import annotations

from numbers import Number

from soda.common.yaml import YamlFile, YamlObject, YamlList


class ContractYaml:

    """
    Represents YAML as close as possible.
    None means the key was not present.
    If property value types do not match the schema, None value will be in the model
    List properties will have a None value if the property is not present or the content was not a list, a list otherwise
    """

    def __init__(self):
        self.contract_yaml_file: YamlFile | None = None
        self.data_source_name: str | None = None
        self.database_name: str | None = None
        self.schema_name: str | None = None
        self.dataset_name: str | None = None
        self.columns: list[ColumnYaml | None] | None = None
        self.checks: list[CheckYaml | None] | None = None


class ValidValuesReferenceDataYaml:

    def __init__(self):
        self.ref_dataset: str | None = None
        self.ref_column: str | None = None


class ColumnYaml:

    def __init__(self):
        self.column_yaml: YamlObject | None = None
        self.name: str | None = None
        self.data_type: str | None = None

        self.missing_values: YamlList = None
        self.missing_regex_sql: str | None = None

        self.invalid_values: list | None = None
        self.invalid_format: str | None = None
        self.invalid_regex_sql: str | None = None
        self.valid_values: list | None = None
        self.valid_format: str | None = None
        self.valid_regex_sql: str | None = None
        self.valid_min: Number | None = None
        self.valid_max: Number | None = None
        self.valid_length: int | None = None
        self.valid_min_length: int | None = None
        self.valid_max_length: int | None = None
        self.valid_values_reference_data: ValidValuesReferenceDataYaml | None = None

        self.checks: list[CheckYaml] | None = None


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
