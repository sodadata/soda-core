from __future__ import annotations

from enum import Enum
from typing import List


class ParserLocation:
    def __init__(self, file_path: str, line: int, column: int):
        self.file_path: str = file_path
        self.line: int = line
        self.column: int = column

    def __str__(self):
        return f"{self.file_path}[{self.line},{self.column}]"


class ParserLogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    ERROR = "ERROR"


class ParserLog:
    def __init__(
        self, level: ParserLogLevel, message: str, location: ParserLocation | None = None, docs_ref: str | None = None
    ):
        self.level: ParserLogLevel = level
        self.message: str = message
        self.location: ParserLocation | None = location
        self.docs_ref: str | None = docs_ref

    def to_assertion_summary(self) -> str:
        docs_txt = (
            f" ->(https://github.com/sodadata/data-contracts/blob/main/docs/{self.docs_ref})"
            if isinstance(self.docs_ref, str)
            else ""
        )
        location_txt = f" {self.location}" if self.location else ""
        return f"{str(self.level.value).ljust(5)} {self.message}{docs_txt}{location_txt}"


class ParserLogs:
    def __init__(self):
        self.logs: list[ParserLog] = []

    def error(self, message: str, location: ParserLocation | None = None, docs_ref: str | None = None):
        self.log(level=ParserLogLevel.ERROR, message=message, location=location, docs_ref=docs_ref)

    def debug(self, message: str, location: ParserLocation | None = None, docs_ref: str | None = None):
        self.log(level=ParserLogLevel.DEBUG, message=message, location=location, docs_ref=docs_ref)

    def info(self, message: str, location: ParserLocation | None = None, docs_ref: str | None = None):
        self.log(level=ParserLogLevel.INFO, message=message, location=location, docs_ref=docs_ref)

    def log(
        self, level: ParserLogLevel, message: str, location: ParserLocation | None = None, docs_ref: str | None = None
    ):
        self.logs.append(ParserLog(level=level, message=message, location=location, docs_ref=docs_ref))
