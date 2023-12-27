from __future__ import annotations

import traceback
from enum import Enum
from textwrap import indent
from typing import List


class Location:
    def __init__(
        self,
        file: str | None = None,
        line: int | None = None,
        column: int | None = None
    ):
        self.file = file
        self.line = line
        self.column = column

    def __str__(self):
        parts =[
            f"file={self.file}" if self.file else None,
            f"line={self.line}" if self.line is not None else None,
            f"column={self.column}" if self.column is not None else None
        ]
        parts = [p for p in parts if p is not None]
        return ",".join(parts)

    def __hash__(self) -> int:
        return hash((self.line, self.column))


class LogLevel(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"


class Log:

    def __init__(self,
                 level: LogLevel,
                 message: str,
                 location: Location | None = None,
                 exception: BaseException | None = None,
                 docs: str | None = None
                 ):
        self.level: LogLevel = level
        self.message: str = message
        self.location: Location | None = location
        self.exception: Exception | None = exception
        self.docs: str | None = docs

    def __str__(self):
        return self.to_string(include_stacktraces=False)

    def to_string(self, include_stacktraces: bool = False) -> str:
        location_str = f" | {self.location}" if self.location else ""
        doc_str = f" | https://go.soda.io/{self.docs}" if self.docs else ""
        exception_str = ""
        if self.exception:
            stacktrace_str = ""
            if include_stacktraces:
                stacktrace_str = "".join(traceback.format_tb(self.exception.__traceback__))
                stacktrace_str = stacktrace_str.strip()
            exception_str = f" | {self.exception}{indent(text=stacktrace_str, prefix='    ')}"
        return f"{self.level.value.ljust(7)}| {self.message}{location_str}{doc_str}{exception_str}"

    @classmethod
    def error(cls,
              message: str,
              location: Location | None = None,
              exception: BaseException | None = None
              ) -> Log:
        return Log(level=LogLevel.ERROR, message=message, location=location, exception=exception)


class Logs:

    def __init__(self, logs: Logs | None = None):
        self.logs: List[Log] = []
        if logs is not None:
            self.logs = logs.logs.copy()

    def assert_no_errors(self) -> None:
        if self.has_errors():
            errors_lines: List[str] = [str(log) for log in self.logs if log.level == LogLevel.ERROR]
            error_text = "\n".join(errors_lines)
            error_word = "error: " if len(self.logs) == 1 else "errors:\n"
            raise AssertionError(f"Connection {error_word}{error_text}")

    def has_errors(self) -> bool:
        return any(log.level == LogLevel.ERROR for log in self.logs)

    def get_errors(self) -> List[Log]:
        return [log for log in self.logs if log.level == LogLevel.ERROR]

    def _log_error(self,
                   message: str,
                   location: Location | None = None,
                   exception: BaseException | None = None
                   ) -> None:
        self._log(Log(LogLevel.ERROR, message, location, exception))

    def _log_warning(self,
                     message: str,
                     location: Location | None = None,
                     exception: BaseException | None = None
                     ) -> None:
        self._log(Log(LogLevel.WARNING, message, location, exception))

    def _log_info(self,
                  message: str,
                  location: Location | None = None,
                  exception: BaseException | None = None
                  ) -> None:
        self._log(Log(LogLevel.INFO, message, location, exception))

    def _log(self, log: Log) -> None:
        self.logs.append(log)
