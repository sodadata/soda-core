from __future__ import annotations

from soda.contracts.parser.parser_log import ParserLogs


class ParserBase:

    def __init__(self, logs: ParserLogs):
        self.logs: ParserLogs = logs
