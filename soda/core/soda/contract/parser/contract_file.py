from __future__ import annotations

from soda.contract.parser.parser_log import ParserLogs
from soda.contract.parser.parser_yaml import YamlString, YamlObject


class ContractFile:

    def __init__(self, contract_yaml_object: YamlObject, file_name: str, logs: ParserLogs):
        self.file_name: str = file_name

        self.name: YamlString | None = contract_yaml_object.read_string_opt("name", logs)
        YamlString.validate_name(self.name, logs)

        self.description: YamlString | None = contract_yaml_object.read_string_opt("description", logs)

        self.datasource: YamlString | None = contract_yaml_object.read_string("datasource", logs)

        self.dataset: YamlString | None = contract_yaml_object.read_string("dataset", logs)

        self.owner: YamlString | None = contract_yaml_object.read_string_opt("owner", logs)
        YamlString.validate_email(self.owner, logs)

        self.schema: Dict[str, ParserDataContractSchemaColumn] | None = None

        schema = self.root_yaml_object.read_object_opt("schema")
        if schema:
            self.schema = {}
            for column_name in schema:
                logging.debug(f"Column {column_name}")
