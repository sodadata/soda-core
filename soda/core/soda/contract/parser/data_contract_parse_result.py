from __future__ import annotations

from soda.contract.data_contract import DataContract
from soda.contract.parser.data_contract_parser_logger import DataContractParserLogger


class DataContractParseResult:
    def __init__(self, contract_yaml_str: str, file_path: str, logger: DataContractParserLogger):
        self.contract_yaml_str: str = contract_yaml_str
        self.file_path: str = file_path
        self.logger: DataContractParserLogger = logger
        self.data_contract: DataContract | None = None
