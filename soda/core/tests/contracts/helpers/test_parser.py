from __future__ import annotations

from textwrap import dedent
from typing import List

from soda.contract.parser.parser_log import ParserLogLevel, ParserLog, ParserLogs, ParserLocation
from soda.contract.parser.contract_parser import ContractParser


class TestParserLogs(ParserLogs):

    def __init__(self):
        super().__init__()
        # See TestParser.continue_on_error
        self.continue_on_error = False

    def error(self, message: str, location: ParserLocation | None = None, docs_ref: str | None = None):
        super().error(message, location, docs_ref)
        if not self.continue_on_error:
            raise AssertionError(f"Expected no error, but got: \n{self.logs[-1].to_assertion_summary()}")


class TestParser(ContractParser):

    def __init__(self):
        super().__init__(TestParserLogs())
        self.next_unnamed_file_index = 0

    def continue_on_error(self) -> TestParser:
        """
        By default the test parser will raise on first error logged to get the direct stack trace in the test runner
        instead of the text logs at the end without a pointer to where the error creation in the codebase
        """
        self.logs.continue_on_error = True
        return self

    def parse(self, contract_yaml_str: str, file_path: str | None = None) -> TestParser:
        if file_path is None:
            self.next_unnamed_file_index += 1
            file_path = f'unnamed_parser_file_{self.next_unnamed_file_index}'
        file_content_str_dedented = dedent(contract_yaml_str)
        super().parse_file_str(file_path, file_content_str_dedented)
        return self

    def assert_no_errors(self):
        self.assert_error_count(0)

    def assert_error_count(self, expected_error_count: int):
        actual_error_count = len(self._collect_logs(ParserLogLevel.ERROR))
        if expected_error_count != actual_error_count:
            raise AssertionError(
                f"Expected error count {expected_error_count}, but was {actual_error_count}. "
                f"{self._collect_all_logs_txt()}"
            )

    def get_error_logs(self) -> List[ParserLog]:
        return [log for log in self.logs.logs if log.level == ParserLogLevel.ERROR]

    def assert_error(self, message_filter: str):
        error_parse_logs = self._collect_logs(
            ParserLogLevel.ERROR,
            message_filter
        )
        if len(error_parse_logs) == 0:
            raise AssertionError(
                f'No error log with message containing "{message_filter}". '
                f'{self._collect_all_logs_txt()}'
            )

        pass

    def _collect_logs(self,
                      level_filter: ParserLogLevel | None = None,
                      message_filter: str | None = None
                      ) -> List[ParserLog]:
        logs: List[ParserLog] = []
        for parse_log in self.logs.logs:
            if ((level_filter is None or parse_log.level == level_filter)
                    and (message_filter is None or message_filter in parse_log.message)):
                logs.append(parse_log)
        return logs

    def _collect_all_logs_txt(self):
        log_txts: List[str] = []
        for parse_log in self.logs.logs:
            log_txts.append(parse_log.to_assertion_summary())
        return "Parser logs:\n" + "\n".join(log_txts)
