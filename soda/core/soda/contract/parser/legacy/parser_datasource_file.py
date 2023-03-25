from __future__ import annotations

from soda.contract.parser.legacy.parser_file import ParserFile
from soda.contract.parser.parser_log import ParserLogs
from soda.contract.parser.parser_yaml import YamlObject, YamlString


class ParserDatasourceFile(ParserFile):

    def __init__(self, logs: ParserLogs, file_path: str, file_content_str: str, root_yaml_object: YamlObject):
        super().__init__(logs=logs, file_path=file_path, file_content_str=file_content_str, root_yaml_object=root_yaml_object)

        self.name: YamlString | None = root_yaml_object.read_string("name")
        YamlString.validate_name(self.name)

        self.type: YamlString | None = root_yaml_object.read_string("type")
        YamlString.validate_id(self.type)

        self.connection: YamlObject | None = None
