"""Log capture for soda-core.

The model, in one read: ONE permanent handler on the root logger
(``_RootCapturer``, re-attached if something like ``logging.basicConfig(force=
True)`` clears the root handlers) routes every record to the *active* ``Logs``
selected by a ContextVar. CONSTRUCTING a ``Logs`` makes it active — capture
needs no extra ceremony and cannot be silently forgotten. ``activate(label)``
re-arms a ``Logs`` for a ``with`` block (the executor brackets each
collection's verify/post-processing with it); ``close()`` flushes the gatherer
and releases the active slot only if this ``Logs`` still holds it. The var is
per-thread, so worker threads that never set it are not captured. Each captured
record's ``thread`` is stamped with the active ``label`` — the per-collection
key Soda Cloud exposes as a log filter.
"""

from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager
from logging import Handler, LogRecord
from typing import Optional

from soda_core.common.logs_base import LogsBase
from soda_core.common.logs_collector import LogsCollector


class Location:
    def __init__(self, file_path: Optional[str], line: Optional[int] = None, column: Optional[int] = None):
        self.file_path: Optional[str] = file_path
        self.line: Optional[int] = line
        self.column: Optional[int] = column

    def __str__(self) -> str:
        src_description: str = self.file_path if self.file_path else "location"
        if self.line is not None and self.column is not None:
            src_description += f"[{self.line},{self.column}]"
        return src_description

    def __hash__(self) -> int:
        return hash((self.line, self.column))

    def get_dict(self) -> dict:
        return {"file_path": self.file_path, "line": self.line, "column": self.column}


_active_logs: contextvars.ContextVar[Optional["Logs"]] = contextvars.ContextVar("soda_active_logs", default=None)


class _RootCapturer(Handler):
    """Routes records to the active ``Logs`` and stamps its ``label`` on
    ``record.thread``. With no active Logs, records pass through to the console
    handler only. (When soda-core is embedded, a host's records reaching root
    during an active scope are captured/re-stamped too — same scope the engine
    has always had.)"""

    def emit(self, record: LogRecord) -> None:
        active: Optional[Logs] = _active_logs.get()
        if active is None:
            return
        if active.label is not None:
            record.thread = active.label
        try:
            active.gatherer.emit(record)
        except Exception:
            self.handleError(record)


def _ensure_root_capturer() -> None:
    """Install the root capturer if it is not currently attached."""
    if not any(isinstance(handler, _RootCapturer) for handler in logging.root.handlers):
        logging.root.addHandler(_RootCapturer())


@contextmanager
def preserve_active_logs():
    """Restore the active capture target to its value at entry on exit. Brackets
    a block that constructs Logs (which activate on construction) so it doesn't
    leave one dangling as the active target afterwards — used by the executor,
    whose per-collection Logs are activated but never individually closed."""
    prev: Optional[Logs] = _active_logs.get()
    try:
        yield
    finally:
        _active_logs.set(prev)


class Logs:
    """Owns a gatherer and is a capture target.

    Constructing a ``Logs`` makes it the active capture target, so
    ``logs = Logs(); logger.info(...)`` captures with no extra ceremony. The
    ``label`` attribute is stamped on each captured record's ``thread`` at emit
    time (combined Cloud uploads group records per collection by it).
    """

    def __init__(self, gatherer: Optional[LogsBase] = None):
        self.gatherer: LogsBase = gatherer if gatherer is not None else LogsCollector()
        self.label: Optional[str] = None
        _ensure_root_capturer()
        self._prev: Optional[Logs] = _active_logs.get()
        _active_logs.set(self)

    @contextmanager
    def activate(self, label: Optional[str] = None):
        """Re-arm this Logs as the active target for a ``with`` block, optionally
        (re)setting its label. The executor uses this to bracket a collection's
        verify() and post-processing after the up-front construction phase has
        moved the active target on to later collections."""
        if label is not None:
            self.label = label
        prev: Optional[Logs] = _active_logs.get()
        _active_logs.set(self)
        try:
            yield self
        finally:
            _active_logs.set(prev)

    def __str__(self) -> str:
        return self.get_logs_str()

    def get_log_records(self) -> list[LogRecord]:
        return self.gatherer.get_all_logs()

    def get_logs(self) -> list[str]:
        return [r.getMessage() for r in self.gatherer.get_all_logs()]

    def get_logs_str(self):
        return "\n".join(self.get_logs())

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def get_errors(self) -> list[str]:
        return [r.getMessage() for r in self.gatherer.get_error_logs()]

    @property
    def has_errors(self) -> bool:
        return len(self.get_errors()) > 0

    def close(self) -> None:
        """Stop being the active capture target and close the gatherer (flushing
        it for streaming gatherers). Releases the active slot only when this
        Logs still holds it, so an out-of-order close (e.g. on a shared
        session-level Logs) leaves a newer active Logs untouched."""
        if _active_logs.get() is self:
            _active_logs.set(self._prev)
        self.gatherer.close()
