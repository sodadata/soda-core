from __future__ import annotations

from soda.contract.parser.data_contract import DataContract
from soda.contract.parser.parser_log import ParserLogs


class DataContractParseResult:

    def __init__(self, contract_yaml_str: str, file_path: str, logs: ParserLogs):
        self.contract_yaml_str: str = contract_yaml_str
        self.file_path: str = file_path
        self.logs: ParserLogs = logs
        self.data_contract: DataContract | None = None
