from __future__ import annotations

import logging
from typing import Dict, List

from soda.contract.check import Check
from soda.contract.column import Column
from soda.contract.parser.parser_helpers import validate_name, validate_email
from soda.contract.parser.parser_log import ParserLogs
from soda.contract.parser.parser_yaml import YamlString, YamlObject


class DataContract:

    @classmethod
    def new(cls) -> DataContract:
        return DataContract()

    @classmethod
    def create_from_yaml(cls, contract_yaml_object: YamlObject, file_path: str, logs: ParserLogs) -> DataContract:
        contract = DataContract()
        contract.file_path = file_path
        contract.name = contract_yaml_object.read_string_opt("name", logs)
        validate_name(logs, contract.name)

        contract.description = contract_yaml_object.read_string_opt("description", logs)

        contract.datasource = contract_yaml_object.read_string("datasource", logs)

        contract.dataset = contract_yaml_object.read_string("name", logs)

        contract.owner = contract_yaml_object.read_string_opt("owner", logs)
        validate_email(logs, contract.owner)

        contract.schema = None

        schema = contract_yaml_object.read_object_opt("schema", logs)
        if schema:
            contract.schema = {}
            for column_name in schema:
                logging.debug(f"Column {column_name}")
                contract.schema[column_name] = Column(
                    name=column_name
                )

        checks = contract_yaml_object.read_list_opt("checks", logs)
        if checks:
            contract.checks = []
            for check in checks:
                logging.debug(f"Check {check}")
                contract.checks.append(
                    Check(
                        check_yaml=check
                    )
                )

        return contract

    def __init__(self):
        self.file_path: YamlString | None = None
        self.name: YamlString | None = None
        self.description: YamlString | None = None
        self.datasource: YamlString | None = None
        self.dataset: YamlString | None = None
        self.owner: YamlString | None = None
        self.schema: Dict[str, Column] | None = None
        self.checks: List[Check] | None = None

    def get_datasource_str(self) -> str | None:
        return self.datasource.value if isinstance(self.datasource, YamlString) else None

    def get_dataset_str(self) -> str | None:
        return self.dataset.value if isinstance(self.dataset, YamlString) else None

    def get_schema_column_names(self) -> List[str]:
        return list(self.schema.keys()) if self.schema else []
