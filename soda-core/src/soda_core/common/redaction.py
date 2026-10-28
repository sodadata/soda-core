"""Redaction of content an org blocks from leaving the engine (today: group-by group values).

Two separate decisions, made in two separate places:

- **What output carries** is stated where it is produced: a log site marks its record with
  ``extra={ExtraKeys.REDACT: RedactionCategory...}``, and code that wraps shared data source code
  (which cannot know what it handles) marks everything inside with ``redacted_scope(...)``.
- **What to do with it** is a ``RedactionPolicy``, built once per check collection from its org
  settings and activated around everything the collection runs. Output sites never see the settings.

Every output goes through the active policy: log records via ``redact_record`` (wired into
``logging_configuration._mask_record``), anything else (a Cloud payload, a stage error) via
``RedactionPolicy.current().redact(...)`` and ``.redact_exception(...)``.
"""

from __future__ import annotations

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from logging import Logger, LogRecord
from typing import TYPE_CHECKING, Iterator, Optional

from soda_core.common.exceptions import get_exception_stacktrace
from soda_core.common.logging_constants import ExtraKeys, RedactionCategory

if TYPE_CHECKING:
    from soda_core.common.soda_cloud_dto import DatasetConfigurationsDTO


def redacted_marker(category: str) -> str:
    """The whole content of a redacted output: no message, no args, no traceback, no
    exception text. The category still tells the reader what was dropped."""
    return f"[redacted: {category}]"


def exception_class_name(exc_type: type) -> str:
    """The exception class a redacted output keeps: the one value-free thing worth knowing."""
    module: str = exc_type.__module__
    return exc_type.__qualname__ if module == "builtins" else f"{module}.{exc_type.__qualname__}"


# Both are read on the thread that produces the output. Handlers mask synchronously on the
# emitting thread, so the caller's context is the one that counts. Work moved to another
# thread does not inherit them: carry them over with ``contextvars.copy_context().run(...)``.
#
# What the output carries: the category of the enclosing ``redacted_scope``.
_redacted_scope: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("soda_redacted_scope", default=None)
# What to do with it: the categories of the active ``RedactionPolicy``.
_active_categories: contextvars.ContextVar[frozenset[str]] = contextvars.ContextVar(
    "soda_redaction_policy", default=frozenset()
)


@contextmanager
def redacted_scope(category: str) -> Iterator[None]:
    """Mark every log record emitted in the block as carrying ``category`` content.

    Marking says what a record contains, not what happens to it: the record is redacted
    only when the active ``RedactionPolicy`` covers the category. Nestable and
    exception-safe; an inner scope's category applies inside it, the outer one is restored
    on exit.
    """
    token = _redacted_scope.set(category)
    try:
        yield
    finally:
        _redacted_scope.reset(token)


@dataclass(frozen=True)
class RedactionPolicy:
    """The ``RedactionCategory`` values a run blocks, and how each kind of output honours them."""

    categories: frozenset[str] = frozenset()

    @classmethod
    def from_datasets_configurations(cls, configs: Optional[DatasetConfigurationsDTO]) -> RedactionPolicy:
        """The policy an org's settings ask for. The one place that maps a setting to a category."""
        categories: set[str] = set()
        if configs is not None and configs.block_group_by_results_to_cloud:
            categories.add(RedactionCategory.GROUP_VALUES)
        return cls(frozenset(categories))

    @classmethod
    def current(cls) -> RedactionPolicy:
        """The policy active on the calling thread: off the verifying thread it is empty unless the
        context was carried over."""
        return cls(_active_categories.get())

    def covers(self, category: str) -> bool:
        return category in self.categories

    @contextmanager
    def activate(self) -> Iterator[RedactionPolicy]:
        """Apply this policy to everything the block emits.

        Nestable and exception-safe, and it only ever adds: an inner policy cannot lift a
        category an outer one redacts. Yields the effective policy.
        """
        effective: frozenset[str] = _active_categories.get() | self.categories
        token = _active_categories.set(effective)
        try:
            yield RedactionPolicy(effective)
        finally:
            _active_categories.reset(token)

    def redact(self, text: str, category: str) -> str:
        """``text`` as it may leave the engine, given that it carries ``category`` content."""
        return redacted_marker(category) if self.covers(category) else text

    def redact_exception(self, exc: BaseException) -> Optional[str]:
        """A raised error as it may leave the engine: its stack trace, or the marker and its class.

        No site marks a raised error, yet its text can carry anything the run handled (e.g. group
        values in a failed-rows query). So it fails closed: any covered category redacts it.
        """
        category: Optional[str] = self._unmarked_category()
        if category is None:
            return get_exception_stacktrace(exc)
        return f"{redacted_marker(category)} {exception_class_name(type(exc))}"

    def log_exception(self, logger: Logger, message: str, exc: BaseException) -> None:
        """Log an error with ``exc``, redacted under the same fail-closed rule as ``redact_exception``.

        Activates this policy itself, so it holds wherever it is called from, including after the
        collection's own ``verify()`` has returned.
        """
        category: Optional[str] = self._unmarked_category()
        if category is None:
            logger.error(message, exc_info=exc)
            return
        with self.activate():
            logger.error(message, exc_info=exc, extra={ExtraKeys.REDACT: category})

    def _unmarked_category(self) -> Optional[str]:
        """The category unmarked output is redacted under, or None when nothing is covered.
        ``min`` only makes the marker deterministic when several categories are covered."""
        return min(self.categories, default=None)


# Set on a record once it has been redacted: a later pass (next handler, a replay outside
# the scope and policy) re-applies the redaction (fail closed) but keeps the exception class
# the first pass derived from the since-dropped ``exc_info``.
_REDACTED_ATTR: str = "redacted"


def redact_record(record: LogRecord) -> bool:
    """Redact the record in place when the active policy covers what it carries. Returns
    whether it did, so the caller skips formatting anything of it."""
    category: Optional[str] = getattr(record, ExtraKeys.REDACT, None) or _redacted_scope.get()
    if category is None:
        return False
    if not (getattr(record, _REDACTED_ATTR, False) or RedactionPolicy.current().covers(category)):
        return False
    _replace_by_marker(record, category)
    return True


def _replace_by_marker(record: LogRecord, category: str) -> None:
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
        exception_class = exception_class_name(record.exc_info[0])
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
