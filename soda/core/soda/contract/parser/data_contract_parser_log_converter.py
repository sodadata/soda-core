from __future__ import annotations

from soda.common.log import LogLevel
from soda.common.logs import Logs
from soda.contract.parser.data_contract_parser_logger import (
    DataContractParserLogger,
    DataContractParserLogLevel,
)
from soda.sodacl.location import Location


class DataContractParserLogConverter(DataContractParserLogger):
    """
    Maps data contract parser logs to scan logs
    """

    def __init__(self, scan_logs: Logs):
        self.scan_logs: Logs = scan_logs

    def log(
        self,
        level: DataContractParserLogLevel,
        message: str,
        file_path: str | None = None,
        line: int | None = None,
        column: int | None = None,
        docs_ref: str | None = None,
    ):
        self.scan_logs.log(
            level=LogLevel[level.value],
            message=message,
            location=Location(file_path=file_path, line=line, col=column) if isinstance(line, int) else None,
            doc=docs_ref,
        )
