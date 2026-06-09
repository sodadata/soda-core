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


# The capture target for the current context. A ``Logs`` becomes active when it
# is constructed and (re)activated via ``Logs.activate()``. Contextvars are
# per-thread, so a thread that never sets it simply isn't captured.
_active_logs: contextvars.ContextVar[Optional["Logs"]] = contextvars.ContextVar("soda_active_logs", default=None)


class _RootCapturer(Handler):
    """The single root-logger handler. While a labelled ``Logs`` is active it
    routes every record reaching the root logger to that Logs' gatherer and
    stamps the Logs' ``label`` on ``record.thread`` (the Cloud per-collection
    grouping key); with no active Logs, records go to the console handler only.
    (When soda-core is embedded, a host's records reaching root during an active
    scope are captured/re-stamped too — same scope the engine has always had.)"""

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


_root_capturer: Optional[_RootCapturer] = None


def _ensure_root_capturer() -> None:
    """Install the single root capturer once."""
    global _root_capturer
    if _root_capturer is None:
        _root_capturer = _RootCapturer()
        logging.root.addHandler(_root_capturer)


@contextmanager
def preserve_active_logs():
    """Restore the active capture target to its value at entry on exit. Brackets
    a block that constructs Logs (which activate on construction) so it doesn't
    leave one dangling as the active target afterwards — used by the executor,
    whose per-collection Logs are activated but never individually closed."""
    token = _active_logs.set(_active_logs.get())
    try:
        yield
    finally:
        try:
            _active_logs.reset(token)
        except (ValueError, LookupError):
            _active_logs.set(None)


class _Activation:
    """Context manager returned by ``Logs.activate()``: makes ``logs`` the
    active target for the block and restores the previous one on exit."""

    def __init__(self, logs: "Logs", label: Optional[str]):
        self._logs = logs
        self._label = label
        self._token: Optional[contextvars.Token] = None

    def __enter__(self) -> "Logs":
        if self._label is not None:
            self._logs.label = self._label
        self._token = _active_logs.set(self._logs)
        return self._logs

    def __exit__(self, *exc) -> None:
        if self._token is not None:
            try:
                _active_logs.reset(self._token)
            except (ValueError, LookupError):
                _active_logs.set(None)
            self._token = None


class Logs:
    """Owns a gatherer and is a capture target.

    Constructing a ``Logs`` makes it the active capture target, so
    ``logs = Logs(); logger.info(...)`` captures with no extra ceremony. The
    optional ``label`` is stamped on each captured record's ``thread`` at emit
    time (combined Cloud uploads group records per collection by it).
    """

    def __init__(self, gatherer: Optional[LogsBase] = None, label: Optional[str] = None):
        self.gatherer: LogsBase = gatherer if gatherer is not None else LogsCollector()
        self.label: Optional[str] = label
        _ensure_root_capturer()
        self._token: Optional[contextvars.Token] = _active_logs.set(self)

    def activate(self, label: Optional[str] = None) -> _Activation:
        """Re-arm this Logs as the active target for a ``with`` block, optionally
        (re)setting its label. The executor uses this to bracket a collection's
        verify() and post-processing after the up-front construction phase has
        moved the active target on to later collections."""
        return _Activation(self, label)

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

    def pop_log_records(self) -> list[LogRecord]:
        # Returns the gatherer's live list (LogsCollector.close is a no-op), so
        # records appended after the pop — e.g. post-processing handler emissions —
        # still surface on the held result. Intentional, not a true "pop".
        log_records: list[LogRecord] = self.get_log_records()
        self.gatherer.close()
        return log_records

    def close(self) -> None:
        """Stop being the active capture target and close the gatherer (flushing
        it for streaming gatherers). Assumes this Logs is the current active
        target — the construct/verify flows keep activations LIFO. ``reset`` is
        guarded so a stray cross-context/out-of-order close degrades to a no-op
        rather than raising; logging must never crash the caller."""
        if self._token is not None:
            try:
                _active_logs.reset(self._token)
            except (ValueError, LookupError):
                _active_logs.set(None)
            self._token = None
        self.gatherer.close()
