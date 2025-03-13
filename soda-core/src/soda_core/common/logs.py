from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone
from logging import DEBUG, ERROR, INFO, WARNING, Logger
from traceback import TracebackException
from typing import Optional


class Emoticons:
    CROSS_MARK: str = "\u274C"
    WHITE_CHECK_MARK: str = "\u2705"
    CLOUD: str = "\u2601"
    OK_HAND: str = "\U0001F44C"
    SCROLL: str = "\U0001F4DC"
    FINGERS_CROSSED: str = "\U0001F91E"
    EXPLODING_HEAD: str = "\U0001F92F"
    POLICE_CAR_LIGHT: str = "\U0001F6A8"
    SEE_NO_EVIL: str = "\U0001F648"
    PINCHED_FINGERS: str = "\U0001F90C"


class Location:
    def __init__(self, file_path: Optional[str], line: Optional[int], column: Optional[int]):
        self.file_path: Optional[str] = file_path
        self.line: Optional[int] = line
        self.column: Optional[int] = column

    def __str__(self) -> str:
        src_description: str = self.file_path if self.file_path else "source file position "
        return f"{src_description}[{self.line},{self.column}]"

    def __hash__(self) -> int:
        return hash((self.line, self.column))

    def get_dict(self) -> dict:
        return {"file_path": self.file_path, "line": self.line, "column": self.column}


class Log:
    def __init__(
        self,
        level: int,
        message: str,
        timestamp: Optional[datetime] = None,
        exception: Optional[BaseException] = None,
        location: Optional[Location] = None,
        doc: Optional[str] = None,
        index: Optional[int] = None,
    ):
        self.level: int = level
        self.message: str = message
        self.timestamp: datetime = timestamp if isinstance(timestamp, datetime) else datetime.now(tz=timezone.utc)
        self.exception: Optional[BaseException] = exception
        self.location: Optional[Location] = location
        self.doc: Optional[str] = doc
        self.index: Optional[int] = index

    def __str__(self):
        location_str = f" | {self.location}" if self.location else ""
        doc_str = f" | see https://go.soda.io/{self.doc}" if self.doc else ""
        exception_str = f" | {self.exception}" if self.exception else ""
        return f"{self.message}{location_str}{doc_str}{exception_str}"

    def get_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "level": self.level,
            "message": self.message,
            "exception": traceback.format_exception(self.exception) if self.exception else None,
            "location": self.location.get_dict() if self.location else None,
            "doc": self.doc if self.doc else None,
            "index": self.index,
        }


class Logs:
    logger: Logger = logging.getLogger("soda.contracts")

    def __init__(self, logs: list[Log] = None):
        # Stores all logs above debug level to be sent to soda cloud and for testing logs in the test suite.
        self.logs: list[Log] = logs or []

    def critical(
        self,
        message: str,
        timestamp: Optional[datetime] = None,
        exception: Optional[BaseException] = None,
        location: Optional[Location] = None,
        doc: Optional[str] = None,
        index: Optional[int] = None,
    ) -> None:
        self.log(
            Log(
                level=logging.CRITICAL,
                message=message,
                timestamp=timestamp,
                exception=exception,
                location=location,
                doc=doc,
                index=index,
            )
        )

    def error(
        self,
        message: str,
        timestamp: Optional[datetime] = None,
        exception: Optional[BaseException] = None,
        location: Optional[Location] = None,
        doc: Optional[str] = None,
        index: Optional[int] = None,
    ) -> None:
        self.log(
            Log(
                level=ERROR,
                message=message,
                timestamp=timestamp,
                exception=exception,
                location=location,
                doc=doc,
                index=index,
            )
        )

    def warning(
        self,
        message: str,
        timestamp: Optional[datetime] = None,
        exception: Optional[BaseException] = None,
        location: Optional[Location] = None,
        doc: Optional[str] = None,
        index: Optional[int] = None,
    ) -> None:
        self.log(
            Log(
                level=WARNING,
                message=message,
                timestamp=timestamp,
                exception=exception,
                location=location,
                doc=doc,
                index=index,
            )
        )

    def info(
        self,
        message: str,
        timestamp: Optional[datetime] = None,
        exception: Optional[BaseException] = None,
        location: Optional[Location] = None,
        doc: Optional[str] = None,
        index: Optional[int] = None,
    ) -> None:
        self.log(
            Log(
                level=INFO,
                message=message,
                timestamp=timestamp,
                exception=exception,
                location=location,
                doc=doc,
                index=index,
            )
        )

    def debug(
        self,
        message: str,
        timestamp: Optional[datetime] = None,
        exception: Optional[BaseException] = None,
        location: Optional[Location] = None,
        doc: Optional[str] = None,
        index: Optional[int] = None,
    ) -> None:
        self.log(
            Log(
                level=DEBUG,
                message=message,
                timestamp=timestamp,
                exception=exception,
                location=location,
                doc=doc,
                index=index,
            )
        )

    def __str__(self) -> str:
        return super().__str__() + "\n".join([str(log) for log in self.logs])

    def has_critical(self) -> bool:
        return any(log.level == logging.CRITICAL for log in self.logs)

    def has_errors(self) -> bool:
        return any(log.level >= ERROR for log in self.logs)

    def get_errors_str(self) -> str:
        error_logs: list[Log] = self.get_errors()
        if len(error_logs) == 0:
            return ""
        return "\n".join([str(log) for log in error_logs])

    def get_errors(self) -> list[Log]:
        return [log for log in self.logs if log.level == ERROR]

    def log(self, log: Log) -> None:
        if log.level >= ERROR:
            log.message = f"{Emoticons.POLICE_CAR_LIGHT} {log.message}"
        self.logs.append(log)
        self.__log_to_python_logging(log)

    def __log_to_python_logging(self, log: Log) -> None:
        extra: dict = {}
        if isinstance(log.location, Location):
            extra["location"] = {
                "file": log.location.file_path,
                "line": log.location.line,
                "column": log.location.column
            }
        if isinstance(log.doc, str):
            extra["doc"] = log.doc

        if log.exception and self.logger.isEnabledFor(DEBUG):
            extra["exception"] = ''.join(TracebackException.from_exception(log.exception).format()).strip()

        self.logger.log(
            level=log.level,
            msg=log.message,
            extra=extra if extra else None
        )
