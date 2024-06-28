from __future__ import annotations

import traceback
from dataclasses import dataclass
from enum import Enum
from textwrap import indent
from typing import List


@dataclass
class Location:

    file_path: str | None
    line: int | None
    column: int | None

    def __str__(self):
        parts = [
            f"line={self.line}" if self.line is not None else None,
            f"column={self.column}" if self.column is not None else None,
            f"file={self.file_path}" if self.file_path is not None else None,
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

    def __init__(
        self,
        level: LogLevel,
        message: str,
        location: Location | None = None,
        exception: BaseException | None = None,
        docs: str | None = None,
    ):
        self.level: LogLevel = level
        self.message: str = message
        self.location: Location | None = location
        self.exception: Exception | None = exception
        self.docs: str | None = docs

    def __str__(self):
        return self.to_string(include_stacktraces=True)

    def to_string(self, include_stacktraces: bool = False) -> str:
        location_str = f" | {self.location}" if self.location else ""
        doc_str = f" | https://go.soda.io/{self.docs}" if self.docs else ""
        exception_str = ""
        if self.exception:
            stacktrace_str = ""
            if include_stacktraces:
                stacktrace_str = "".join(traceback.format_tb(self.exception.__traceback__))
                stacktrace_str = stacktrace_str.strip()
            exception_str = f" | {self.exception}\n{indent(text=stacktrace_str, prefix='    ')}"
        return f"{self.level.value.ljust(7)}| {self.message}{location_str}{doc_str}{exception_str}"

    @classmethod
    def error(cls, message: str, location: Location | None = None, exception: BaseException | None = None) -> Log:
        return Log(level=LogLevel.ERROR, message=message, location=location, exception=exception)


class Logs:

    # See also adr/03_exceptions_vs_error_logs.md

    def __init__(self, logs: Logs | None = None):
        self.logs: List[Log] = []
        if logs is not None:
            self.logs = logs.logs.copy()

    def __str__(self) -> str:
        return "\n".join([str(log) for log in self.logs])

    def has_errors(self) -> bool:
        return any(log.level == LogLevel.ERROR for log in self.logs)

    def get_errors_str(self) -> str:
        errors_lines: List[str] = [str(log) for log in self.logs if log.level == LogLevel.ERROR]
        error_text = "\n".join(errors_lines)
        error_word = "Error: " if len(self.logs) == 1 else "Errors:\n"
        return f"{error_word}{error_text}"

    def get_errors(self) -> List[Log]:
        return [log for log in self.logs if log.level == LogLevel.ERROR]

    def error(self, message: str, location: Location | None = None, exception: BaseException | None = None) -> None:
        self._log(Log(LogLevel.ERROR, message, location, exception))

    def info(self, message: str, location: Location | None = None, exception: BaseException | None = None) -> None:
        self._log(Log(LogLevel.INFO, message, location, exception))

    def _log(self, log: Log) -> None:
        self.logs.append(log)
