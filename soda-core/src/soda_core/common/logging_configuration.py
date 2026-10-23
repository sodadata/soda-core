import contextvars
import hashlib
import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from logging import CRITICAL, DEBUG, ERROR, INFO, WARNING, Formatter, LogRecord, StreamHandler
from typing import Iterable, Iterator, Optional

from soda_core.common.logging_constants import Emoticons, ExtraKeys

verbose_mode: bool = False


def configure_logging(
    verbose: bool = False,
) -> None:
    """
    Used exposing the Soda log configurations.
    """
    global verbose_mode
    verbose_mode = verbose

    _prepare_masked_file()

    sys.stderr = sys.stdout
    for logger_to_mute in [
        "urllib3",
        "botocore",
        "pyathena",
        "faker",
        "snowflake",
        "matplotlib",
        "pyspark",
        "pyhive",
        "py4j",
        "segment",
    ]:
        logging.getLogger(logger_to_mute).setLevel(ERROR)

    soda_log_level: int = DEBUG if verbose else INFO

    logging.basicConfig(
        level=soda_log_level,
        force=True,  # Override any previously set handlers.
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        handlers=[SodaConsoleHandler()],
    )

    # force=True just cleared every root handler, including the capture router.
    # Re-attach it so any live ``Logs`` keeps capturing. Function-local import:
    # ``logs`` imports (via logs_collector) from this module.
    from soda_core.common.logs import _ensure_root_capturer

    _ensure_root_capturer()


_masked_values = set()


def _prepare_masked_file():
    global _masked_values
    file_with_masked_values = os.environ.get("SODA_MASKED_VALUES_FILE", None)
    if file_with_masked_values is not None:
        if not os.path.exists(file_with_masked_values):
            raise RuntimeError(f"Masked values file '{file_with_masked_values}' does not exist")
        sha_expected = os.environ.get("SODA_MASKED_VALUES_FILE_HASH", None)
        if not sha_expected:
            raise RuntimeError(f"Hash for the masked values file '{file_with_masked_values}' is not present")
        sha_hash = hashlib.sha256()
        with open(file_with_masked_values) as f:
            _masked_values.clear()
            read_lines = [l for l in f.readlines()]
            for l in read_lines:
                sha_hash.update(l.encode("utf-8"))
            sha_actual = sha_hash.hexdigest()
            if sha_actual != sha_expected:
                raise RuntimeError(
                    f"Hash mismatch for the masked values file '{file_with_masked_values}' (expected '{sha_expected}', got '{sha_actual}')"
                )
            _masked_values.update({l.strip() for l in read_lines})


def redacted_marker(category: str) -> str:
    """The whole content of a redacted record: no message, no args, no traceback, no
    exception text. The category still tells the reader what was dropped."""
    return f"[redacted: {category}]"


# Both are read at mask time on the thread that emits the record. Handlers mask synchronously
# on the emitting thread, so the caller's context is the one that counts. Work moved to
# another thread does not inherit them: carry them over with ``contextvars.copy_context().run(...)``.
#
# What the record carries: the category of the enclosing ``redacted_scope``, for the log
# sites that cannot know what they handle (query result rows, warehouse inserts, driver
# and connector errors). The code that wraps them does know.
_redacted_scope: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("soda_redacted_scope", default=None)
# What to do with it: the categories the current run redacts, set once by the collection.
_redaction_policy: contextvars.ContextVar[frozenset[str]] = contextvars.ContextVar(
    "soda_redaction_policy", default=frozenset()
)


@contextmanager
def redacted_scope(category: str) -> Iterator[None]:
    """Mark every log record emitted in the block as carrying ``category`` content.

    Marking says what a record contains, not what happens to it: the record is redacted
    only when the active ``redaction_policy`` covers the category. Nestable and
    exception-safe; an inner scope's category applies inside it, the outer one is restored
    on exit.
    """
    token = _redacted_scope.set(category)
    try:
        yield
    finally:
        _redacted_scope.reset(token)


@contextmanager
def redaction_policy(categories: Iterable[str]) -> Iterator[None]:
    """Redact, for the block, every record marked with one of ``categories``.

    Set once per run by the check collection from its org settings; the log sites never
    see those. Nestable and exception-safe, and it only ever adds: an inner policy cannot
    lift a category the outer one redacts. An empty ``categories`` leaves the block as it is.
    """
    outer: frozenset[str] = _redaction_policy.get()
    token = _redaction_policy.set(outer | frozenset(categories))
    try:
        yield
    finally:
        _redaction_policy.reset(token)


# Set on a record once it has been redacted: a later pass (next handler, a replay outside
# the scope and policy) re-applies the redaction (fail closed) but keeps the exception class
# the first pass derived from the since-dropped ``exc_info``.
_REDACTED_ATTR: str = "redacted"


def _category_to_redact(record: LogRecord) -> Optional[str]:
    """The category the record is to be redacted under, or None to leave it as it is."""
    category: Optional[str] = getattr(record, ExtraKeys.REDACT, None) or _redacted_scope.get()
    if category is None:
        return None
    if getattr(record, _REDACTED_ATTR, False) or category in _redaction_policy.get():
        return category
    return None


def _redact(record: LogRecord, category: str) -> None:
    """Replace the record by the marker, before anything of it is formatted.

    Clears every field a handler renders or uploads: ``msg``/``args`` (the message),
    ``exc_info``/``exc_text`` (the console traceback) and the ``ExtraKeys.EXCEPTION``
    extra (the only exception field the Cloud upload carries), keeping the exception
    class as the one value-free thing worth knowing. Stamps ``ExtraKeys.REDACT`` with the
    category so a later handler, on another thread or replaying the record outside the
    scope, recognises it without the scope.
    """
    exception_class: Optional[str] = None
    if isinstance(record.exc_info, tuple) and record.exc_info[0] is not None:
        exc_type = record.exc_info[0]
        module: str = exc_type.__module__
        exception_class = exc_type.__qualname__ if module == "builtins" else f"{module}.{exc_type.__qualname__}"
    record.msg = redacted_marker(category)
    record.args = ()
    record.exc_info = None
    record.exc_text = None
    if exception_class is not None:
        setattr(record, ExtraKeys.EXCEPTION, exception_class)
    elif hasattr(record, ExtraKeys.EXCEPTION) and not getattr(record, _REDACTED_ATTR, False):
        setattr(record, ExtraKeys.EXCEPTION, None)
    setattr(record, ExtraKeys.REDACT, category)
    setattr(record, _REDACTED_ATTR, True)


def _mask_record(record: LogRecord):
    category: Optional[str] = _category_to_redact(record)
    if category is not None:
        _redact(record, category)
        return
    message = record.getMessage()
    if message:
        for masked in _masked_values:
            if (not masked) or masked not in message:
                continue
            message = message.replace(masked, "***")
    record.msg = message
    # since getMessage evaluates args, we need to clear them after the full message has been cleared
    record.args = ()
    if record.exc_info:
        exc_type, exc_value, exc_tb = record.exc_info
        try:
            masked_exc = _mask_exception(exc_value)
        except MaskingError:
            # Masking is a security control, not best-effort: an exception whose
            # secrets could not be masked must never reach a handler. The record
            # itself is kept (its message is already masked above, and an ERROR must
            # stay an ERROR so check outcomes are not silently downgraded), but the
            # exception and any cached traceback text are dropped entirely.
            record.exc_info = None
            record.exc_text = None
            record.msg = f"{record.msg} {REDACTED_EXCEPTION_MARKER}"
        else:
            record.exc_info = (exc_type, masked_exc, exc_tb)
    else:
        record.exc_info = None


def _mask_message(message: Optional[str]) -> Optional[str]:
    if not message or not _masked_values:
        return message
    for masked in _masked_values:
        if (not masked) or masked not in message:
            continue
        message = message.replace(masked, "***")
    return message


class MaskingError(Exception):
    """Secret masking of an exception could not be completed.

    Raised by ``_mask_exception`` instead of letting the underlying error escape: the
    original error is deliberately not chained (``from None``) because its traceback
    frames are exactly what could not be masked.
    """


REDACTED_EXCEPTION_MARKER = "[exception redacted: secret masking failed]"


def _mask_exception(e: Optional[BaseException]) -> Optional[BaseException]:
    """Mask registered secrets in an exception's args and traceback frame locals.

    Raises ``MaskingError`` if any part of the exception could not be masked. Callers
    must then treat the whole exception as unmaskable and drop it (see ``_mask_record``)
    rather than log it partially masked.
    """
    if e is None:
        return None

    try:
        # Mask the exception's args (usually contains the error message)
        if e.args:
            masked_args = []
            for arg in e.args:
                if isinstance(arg, str):
                    masked_arg = _mask_message(arg)
                    masked_args.append(masked_arg)
                else:
                    masked_args.append(arg)
            e.args = tuple(masked_args)

        # Mask the traceback frames
        tb = e.__traceback__
        while tb is not None:
            frame = tb.tb_frame
            # Mask local variables in the frame that are strings.
            # Iterate over a snapshot (``list(...)``): assigning ``frame.f_locals[...]``
            # re-syncs the frame's fast locals, mutating the very mapping being iterated,
            # which raises "dictionary keys changed during iteration" on Python < 3.13 for
            # some frames (e.g. a bound-method local plus a closure frame, the shape the
            # SQL Server deadlock-retry wrapper introduced).
            if frame.f_locals:
                for var_name, var_value in list(frame.f_locals.items()):
                    if isinstance(var_value, str):
                        frame.f_locals[var_name] = _mask_message(var_value)
            tb = tb.tb_next

        # Recursively mask chained exceptions
        if e.__cause__ is not None:
            _mask_exception(e.__cause__)
        if e.__context__ is not None:
            _mask_exception(e.__context__)
    except MaskingError:
        raise
    except Exception as masking_failure:
        raise MaskingError(f"Could not mask secrets in {type(e).__name__}: {type(masking_failure).__name__}") from None
    return e


def is_verbose() -> bool:
    return verbose_mode


class SodaConsoleHandler(StreamHandler):
    def __init__(self):
        super().__init__(sys.stdout)
        self.setFormatter(SodaConsoleFormatter())


class SodaConsoleFormatter(Formatter):
    def __init__(self):
        super().__init__()

    def format(self, record) -> str:
        parts: list[str] = [
            # self.format_timestamp(record),
            # self.format_level(record),
            self.format_message(record),
            self.format_location(record),
            self.format_doc(record),
            self.format_exception(record),
        ]
        return " | ".join(part for part in parts if part is not None)

    level_names: dict[int, str] = {
        CRITICAL: "CRI",
        ERROR: "ERR",
        WARNING: "WAR",
        INFO: "INF",
        DEBUG: "DEB",
    }

    def format_level(self, record: LogRecord) -> Optional[str]:
        return self.level_names.get(record.levelno, "UNKNOWN")

    def format_message(self, record: LogRecord) -> str:
        _mask_record(record)
        if record.levelno >= ERROR:
            return f"{Emoticons.POLICE_CAR_LIGHT} {record.getMessage()}"
        else:
            return record.getMessage()

    def format_timestamp(self, record: LogRecord) -> Optional[str]:
        timestamp: datetime = datetime.fromtimestamp(record.created)
        # Format the time part (without milliseconds)
        time_part = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        # Add milliseconds with comma as separator
        milliseconds = timestamp.microsecond // 1000  # Convert microseconds to milliseconds
        return f"{time_part},{milliseconds:03d}"

    def format_location(self, record: LogRecord) -> Optional[str]:
        if hasattr(record, ExtraKeys.LOCATION):
            return str(record.location)
        else:
            return None

    def format_doc(self, record: LogRecord) -> Optional[str]:
        if hasattr(record, ExtraKeys.DOC):
            return record.doc
        else:
            return None

    def format_exception(self, record: LogRecord) -> Optional[str]:
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = _mask_message(self.formatException(record.exc_info))
                return record.exc_text

        if hasattr(record, ExtraKeys.EXCEPTION):
            return record.exception
