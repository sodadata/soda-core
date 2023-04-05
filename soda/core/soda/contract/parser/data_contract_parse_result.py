from __future__ import annotations

from soda.contract.data_contract import DataContract
from soda.contract.parser.data_contract_parser_logger import DataContractParserLogger


class DataContractParseResult:
<<<<<<< HEAD
    def __init__(self, contract_yaml_str: str, file_path: str, logs: ParserLogs):
=======

    def __init__(self, contract_yaml_str: str, file_path: str, logger: DataContractParserLogger):
>>>>>>> 54dd0f18 (Contracts cleanup)
        self.contract_yaml_str: str = contract_yaml_str
        self.file_path: str = file_path
        self.logger: DataContractParserLogger = logger
        self.data_contract: DataContract | None = None
