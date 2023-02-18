from __future__ import annotations

from typing import List, Dict

from ruamel.yaml import YAML, CommentedMap, CommentedSeq
from ruamel.yaml.error import MarkedYAMLError

from soda.contracts.parser.parser_base import ParserBase
from soda.contracts.parser.parser_data_contract_file import ParserDataContractFile
from soda.contracts.parser.parser_datasource_file import ParserDatasourceFile
from soda.contracts.parser.parser_file import ParserFile
from soda.contracts.parser.parser_log import ParserLogs, ParserLocation
from soda.contracts.parser.parser_plugin import ParserPlugin
from soda.contracts.parser.parser_resolver import ParserResolver
from soda.contracts.parser.parser_yaml import YamlObject, YamlString


class Parser(ParserBase):

    def __init__(self,
                 logs: ParserLogs = ParserLogs(),
                 variable_resolver: ParserResolver = ParserResolver()):
        super().__init__(logs)
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.files: List[ParserFile] = []
        self.plugins: List[ParserPlugin] = []
        self.variable_resolver: ParserResolver = variable_resolver

    def _get_file_type(self, file_path: str, root_ruamel_map: CommentedMap) -> type | None:
        if "dataset" in root_ruamel_map:
            return ParserDataContractFile
        if "connection" in root_ruamel_map:
            return ParserDatasourceFile
        return None

    def parse_file_str(self, file_path: str, file_content_str: str) -> None:
        resolved_file_content_str = self.variable_resolver.resolve_variables(file_content_str)

        self.logs.debug(f"Parsing file '{file_path}'")
        root_ruamel_object: CommentedMap = self._parse_yaml_str(
            file_path=file_path,
            file_content_str=resolved_file_content_str
        )

        if not isinstance(root_ruamel_object, CommentedMap):
            actual_type_name = "list" \
                if isinstance(root_ruamel_object, CommentedSeq) \
                else type(root_ruamel_object).__name__
            self.logs.error(
                message=f"All top level YAML elements must be objects, but was '{actual_type_name}'",
                docs_ref="04-data-contract-language.md#file-type"
            )
            return None

        file_type = self._get_file_type(file_path=file_path, root_ruamel_map=root_ruamel_object)
        if file_type is None:
            self.logs.error(f"File type could not be determined for {file_path}")
            return

        root_yaml_object = YamlObject(
            ruamel_object=root_ruamel_object,
            logs=self.logs,
            location=ParserLocation(file_path, 0, 0)
        )

        parser_file = file_type(
            logs=self.logs,
            file_path=file_path,
            file_content_str=file_content_str,
            root_yaml_object=root_yaml_object,
        )
        self.files.append(parser_file)

        for plugin in self.plugins:
            plugin.parse(file=parser_file)

    def _parse_yaml_str(self, file_path: str, file_content_str: str):
        try:
            return self.yaml.load(file_content_str)
        except MarkedYAMLError as e:
            self.logs.error(
                message=f"Invalid YAML: {str(e)}",
                location=ParserLocation(file_path=file_path, line=e.problem_mark.line, column=e.problem_mark.column)
            )

    def validate_semantics(self):
        """
        To be called after all files have been parsed and loaded into the parser.
        While the parse_file_str method above will already validate YAML, known YAML schema
        and value formats, this method will validate the files semantically without a connection
        to a data source.  This includes
        * Checking for duplicate datasource names
        * Checking for undefined datasource references
        * TODO Checking for multiple data contracts on the same dataset
        * ...
        """
        self._validate_datasource_names()

    def _validate_datasource_names(self):
        """
        Checks for duplicate declarations of datasource names
        and if referenced datasource names are present
        """
        # First collect all the datasource declarations and their location
        # maps datasource names to locations where they are defined
        datasource_locations: Dict[str, List[ParserLocation]] = {}
        for file in self.files:
            if isinstance(file, ParserDatasourceFile):
                if isinstance(file.name, YamlString):
                    datasource_name: str = file.name.value
                    datasource_locations.setdefault(datasource_name, [])
                    datasource_locations[datasource_name].append(file.name.location)

        duplicate_declared_datasources: Dict[str, List[ParserLocation]] = {
            k: v for k, v in datasource_locations.items() if len(datasource_locations[k]) > 1
        }
        for duplicate_declared_datasource_name in duplicate_declared_datasources:
            locations: List[ParserLocation] = duplicate_declared_datasources[duplicate_declared_datasource_name]
            for location in locations:
                self.logs.error(
                    message=f"Datasource '{duplicate_declared_datasource_name}' was declared {len(locations)} times",
                    location=location
                )

        declared_datasource_names = datasource_locations.keys()
        for file in self.files:
            if isinstance(file, ParserDataContractFile):
                if isinstance(file.datasource, YamlString):
                    datasource_reference = file.datasource.value
                    if datasource_reference not in declared_datasource_names:
                        self.logs.error(
                            message=f"Datasource '{datasource_reference}' is not defined",
                            location=file.datasource.location
                        )
