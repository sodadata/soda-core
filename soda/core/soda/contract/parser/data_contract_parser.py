from __future__ import annotations

from ruamel.yaml import YAML, CommentedMap, CommentedSeq
from ruamel.yaml.error import MarkedYAMLError
from soda.contract.data_contract import DataContract
from soda.contract.parser.data_contract_parse_result import DataContractParseResult
from soda.contract.parser.data_contract_parser_logger import DataContractParserLogger
from soda.contract.parser.data_contract_variable_resolver import (
    DataContractVariableResolver,
)
from soda.contract.parser.data_contract_yaml import YamlLocation, YamlObject


class DataContractParser:
    """
    Parses data contract YAML files.

    Usage:
    parser = ContractParser()
    data_contract_parse_result = parser.parse(contract_yaml_str_1, file_path_1, logs)
    data_contract_parse_result = parser.parse(contract_yaml_str_2, file_path_2, logs)

    ContractParser is immutable, so it's thread safe.
    """

    def __init__(self, variable_resolver: DataContractVariableResolver = DataContractVariableResolver()):
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.variable_resolver: DataContractVariableResolver = variable_resolver

    def parse(
        self, contract_yaml_str: str, file_path: str, logger: DataContractParserLogger = DataContractParserLogger()
    ) -> DataContractParseResult:
        """
        Parses a contract file YAML file_content_str and builds up the corresponding ContractFile python data structure.
        :param contract_yaml_str: The YAMl text as a python string
        :param file_path:
        :param logger:
        :return: A DataContractParseResult
        """
        data_contract_parse_result = DataContractParseResult(
            contract_yaml_str=contract_yaml_str, file_path=file_path, logger=logger
        )

        resolved_file_content_str = self.variable_resolver.resolve_variables(contract_yaml_str)

        logger.debug(f"Parsing file '{file_path}'")
        root_ruamel_object: CommentedMap = self._parse_yaml_str(
            file_path=file_path, file_content_str=resolved_file_content_str, logs=logger
        )

        if isinstance(root_ruamel_object, CommentedMap):
            root_yaml_object = YamlObject(
                ruamel_value=root_ruamel_object, location=YamlLocation(file_path, 0, 0), logs=logger
            )

            data_contract_parse_result.data_contract = DataContract.create_from_yaml(
                contract_yaml_object=root_yaml_object, file_path=file_path, logs=logger
            )

        else:
            actual_type_name = (
                "list" if isinstance(root_ruamel_object, CommentedSeq) else type(root_ruamel_object).__name__
            )
            logger.error(
                message=f"All top level YAML elements must be objects, but was '{actual_type_name}'",
                docs_ref="04-data-contract-language.md#file-type",
            )

        return data_contract_parse_result

    def _parse_yaml_str(self, file_path: str, file_content_str: str, logs: DataContractParserLogger):
        try:
            return self.yaml.load(file_content_str)
        except MarkedYAMLError as e:
            logs.error(
                message=f"Invalid YAML: {str(e)}",
                location=YamlLocation(file_path=file_path, line=e.problem_mark.line, column=e.problem_mark.column),
            )
