from __future__ import annotations

import logging
from typing import Dict

from soda.contracts.parser.parser_file import ParserFile
from soda.contracts.parser.parser_log import ParserLogs
from soda.contracts.parser.parser_yaml import YamlObject, YamlString


class ParserDataContractSchemaColumn:
    pass


class ParserDataContractFile(ParserFile):

    def __init__(self, logs: ParserLogs, file_path: str, file_content_str: str, root_yaml_object: YamlObject):
        super().__init__(logs=logs, file_path=file_path, file_content_str=file_content_str, root_yaml_object=root_yaml_object)

        self.name: YamlString | None = root_yaml_object.read_string_opt("name")
        YamlString.validate_name(self.name)

        self.description: YamlString | None = root_yaml_object.read_string_opt("description")

        self.datasource: YamlString | None = root_yaml_object.read_string("datasource")

        self.dataset: YamlString | None = self.root_yaml_object.read_string("dataset")

        self.owner: YamlString | None = root_yaml_object.read_string_opt("owner")
        YamlString.validate_email(self.owner)

        self.schema: Dict[str, ParserDataContractSchemaColumn] | None = None

        schema = self.root_yaml_object.read_object_opt("schema")
        if schema:
            self.schema = {}
            for column_name in schema:
                logging.debug(f"Column {column_name}")
