from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone
from logging import getLevelName, ERROR, WARNING, INFO, DEBUG, Logger


class Location:

    def __init__(
            self,
            file_path: str | None,
            line: int | None,
            column: int | None
    ):
        self.file_path: str | None = file_path
        self.line: int | None = line
        self.column: int | None = column

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

    def get_dict(self) -> dict:
        return {
            "file_path": self.file_path,
            "line": self.line,
            "column": self.column
        }


class Log:

    def __init__(
        self,
        level: int,
        message: str,
        timestamp: datetime | None = None,
        exception: BaseException | None = None,
        location: Location | None = None,
        doc: str | None = None,
        index: int | None = None
    ):
        self.level: int = level
        self.message: str = message
        self.timestamp: datetime = timestamp if isinstance(timestamp, datetime) else datetime.now(tz=timezone.utc)
        self.exception: BaseException | None = exception
        self.location: Location | None = location
        self.doc: str | None = doc
        self.index: int | None = index

    def __str__(self):
        location_str = f" | {self.location}" if self.location else ""
        doc_str = f" | https://go.soda.io/{self.doc}" if self.doc else ""
        exception_str = f" | {self.exception}" if self.exception else ""
        level_name: str = getLevelName(self.level)
        return f"{level_name:<7}| {self.message}{location_str}{doc_str}{exception_str}"

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

    def __init__(self):
        # Stores all logs above debug level to be sent to soda cloud and for testing logs in the test suite.
        self.logs: list[Log] = []

    def error(
            self,
            message: str,
            timestamp: datetime | None = None,
            exception: BaseException | None = None,
            location: Location | None = None,
            doc: str | None = None,
            index: int | None = None
            ) -> None:
        self.__log(
            Log(
                level=ERROR, message=message, timestamp=timestamp, exception=exception, location=location, doc=doc,
                index=index
            )
        )

    def warning(
            self,
            message: str,
            timestamp: datetime | None = None,
            exception: BaseException | None = None,
            location: Location | None = None,
            doc: str | None = None,
            index: int | None = None
            ) -> None:
        self.__log(
            Log(
                level=WARNING, message=message, timestamp=timestamp, exception=exception, location=location, doc=doc,
                index=index
            )
        )

    def info(
            self,
            message: str,
            timestamp: datetime | None = None,
            exception: BaseException | None = None,
            location: Location | None = None,
            doc: str | None = None,
            index: int | None = None
            ) -> None:
        self.__log(
            Log(
                level=INFO, message=message, timestamp=timestamp, exception=exception, location=location, doc=doc,
                index=index
            )
        )

    def debug(
            self,
            message: str,
            timestamp: datetime | None = None,
            exception: BaseException | None = None,
            location: Location | None = None,
            doc: str | None = None,
            index: int | None = None
    ) -> None:
        self.__log(
            Log(
                level=DEBUG, message=message, timestamp=timestamp, exception=exception, location=location, doc=doc,
                index=index
            )
        )

    def __str__(self) -> str:
        return "\n".join([str(log) for log in self.logs])

    def has_errors(self) -> bool:
        return any(log.level == ERROR for log in self.logs)

    def get_errors_str(self) -> str:
        error_logs: list[Log] = self.get_errors()
        if len(error_logs) == 0:
            return ""
        return "\n".join([str(log) for log in error_logs])

    def get_errors(self) -> list[Log]:
        return [log for log in self.logs if log.level == ERROR]

    def __log(self, log: Log) -> None:
        if log.level > DEBUG:
            self.logs.append(log)
        self.__log_to_python_logging(log)

    def __log_to_python_logging(self, log: Log) -> None:
        message = log.message
        if log.location:
            message = f"{message}\n at {log.location}"
        if log.doc:
            message = f"{message}\n see https://go.soda.io/{log.doc}"
        self.logger.log(
            level=log.level,
            msg=message,
            exc_info=log.exception,
            stack_info=log.exception is not None,
        )