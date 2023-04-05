from __future__ import annotations

from enum import Enum


class DataContractParserLogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    ERROR = "ERROR"


class DataContractParserLogger:
    def error(
        self,
        message: str,
        file_path: str | None = None,
        line: int | None = None,
        column: int | None = None,
        docs_ref: str | None = None,
    ):
        self.log(
            level=DataContractParserLogLevel.ERROR,
            message=message,
            file_path=file_path,
            line=line,
            column=column,
            docs_ref=docs_ref,
        )

    def debug(
        self,
        message: str,
        file_path: str | None = None,
        line: int | None = None,
        column: int | None = None,
        docs_ref: str | None = None,
    ):
        self.log(
            level=DataContractParserLogLevel.DEBUG,
            message=message,
            file_path=file_path,
            line=line,
            column=column,
            docs_ref=docs_ref,
        )

    def info(
        self,
        message: str,
        file_path: str | None = None,
        line: int | None = None,
        column: int | None = None,
        docs_ref: str | None = None,
    ):
        self.log(
            level=DataContractParserLogLevel.INFO,
            message=message,
            file_path=file_path,
            line=line,
            column=column,
            docs_ref=docs_ref,
        )

    def log(
        self,
        level: DataContractParserLogLevel,
        message: str,
        file_path: str | None = None,
        line: int | None = None,
        column: int | None = None,
        docs_ref: str | None = None,
    ):
        docs_txt = (
            f" ->(https://github.com/sodadata/data-contracts/blob/main/docs/{docs_ref})"
            if isinstance(docs_ref, str)
            else ""
        )
        location_txt = f" {file_path}[{line},{column}]" if file_path and line else ""
        log_line = f"{str(level.value).ljust(5)} {message}{docs_txt}{location_txt}"
        print(log_line)
