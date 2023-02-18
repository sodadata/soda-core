from __future__ import annotations

from soda.contracts.parser.parser_base import ParserBase
from soda.contracts.parser.parser_log import ParserLogs
from soda.contracts.parser.parser_yaml import YamlObject


class ParserFile(ParserBase):

    def __init__(self, logs: ParserLogs, file_path: str, file_content_str: str, root_yaml_object: YamlObject):
        super().__init__(logs)
        self.file_path: str = file_path
        self.file_content_str: str = file_content_str
        self.root_yaml_object: YamlObject = root_yaml_object
