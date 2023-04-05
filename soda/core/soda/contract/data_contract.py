from __future__ import annotations

import logging

from soda.contract.check import Check
from soda.contract.column import Column
from soda.contract.parser.data_contract_parser_logger import DataContractParserLogger
from soda.contract.parser.data_contract_parser_validators import validate_name, validate_email
from soda.contract.parser.data_contract_yaml import YamlString, YamlObject


class DataContract:
    @classmethod
    def new(cls) -> DataContract:
        return DataContract()

    @classmethod
    def create_from_yaml(cls, contract_yaml_object: YamlObject, file_path: str, logs: DataContractParserLogger) -> DataContract:
        data_contract = DataContract()
        data_contract.file_path = file_path
        data_contract.yaml = contract_yaml_object
        data_contract.name = contract_yaml_object.read_string_opt("name", logs)
        validate_name(logs, data_contract.name)

        data_contract.description = contract_yaml_object.read_string_opt("description", logs)

        data_contract.datasource = contract_yaml_object.read_string("datasource", logs)

        data_contract.dataset = contract_yaml_object.read_string("name", logs)

        data_contract.owner = contract_yaml_object.read_string_opt("owner", logs)
        validate_email(logs, data_contract.owner)

        data_contract.schema = None

        schema = contract_yaml_object.read_object_opt("schema", logs)
        if schema:
            data_contract.schema_location = schema.location
            data_contract.schema = {}
            for column_name in schema:
                logging.debug(f"Column {column_name}")
                data_contract.schema[column_name] = Column(
                    name=column_name
                )

        checks = contract_yaml_object.read_list_opt("checks", logs)
        if checks:
            data_contract.checks = []
            for check in checks:
                logging.debug(f"Check {check}")
                data_contract.checks.append(
                    Check(
                        check_yaml=check
                    )
                )

        return data_contract

    def __init__(self):
        self.file_path: YamlString | None = None
        self.yaml: YamlObject | None = None,
        self.name: YamlString | None = None
        self.description: YamlString | None = None
        self.datasource: YamlString | None = None
        self.dataset: YamlString | None = None
        self.owner: YamlString | None = None
        self.schema: dict[str, Column] | None = None
        self.checks: list[Check] | None = None

    def get_datasource_str(self) -> str | None:
        return self.datasource.value if isinstance(self.datasource, YamlString) else None

    def get_dataset_str(self) -> str | None:
        return self.dataset.value if isinstance(self.dataset, YamlString) else None

    def get_schema_column_names(self) -> list[str]:
        return list(self.schema.keys()) if self.schema else []

    def get_ruamel_yaml_object(self) -> object | None:
        return self.yaml.ruamel_value if self.yaml else None
