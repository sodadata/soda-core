from __future__ import annotations

import logging

import pytest
from soda_core.common import logging_configuration
from soda_core.common.logging_configuration import (
    REDACTED_EXCEPTION_MARKER,
    MaskingError,
    SodaConsoleFormatter,
    _mask_exception,
    _mask_record,
    _masked_values,
    is_redacted,
    redacted_marker,
    redacted_scope,
    redaction_policy,
)
from soda_core.common.logging_constants import RedactionCategory


class _FakeRetryingConnection:
    """Mirrors the traceback shape of ``SqlServerDataSourceConnection.execute_query``:

        execute_query -> _execute_with_deadlock_retry -> lambda -> super().execute_query

    i.e. a bound-method captured in a local (``execute = self._base_execute``) plus a
    closure frame. That combination made ``_mask_exception`` raise
    ``RuntimeError: dictionary keys changed during iteration`` on Python 3.10 — masking a
    frame's locals re-syncs ``frame.f_locals`` and invalidates the iterator. The raise
    happened inside the logging handler, which dropped the log record and silently
    swallowed the underlying error (so the failing check ended up NOT_EVALUATED).
    """

    def _execute_with_retry(self, operation):
        return operation()

    def execute_query(self, sql: str, log_query: bool = True):
        execute = self._base_execute
        return self._execute_with_retry(lambda: execute(sql, log_query))

    def _base_execute(self, sql: str, log_query: bool = True):
        raise ValueError(f"Invalid column name in {sql}")


def test_mask_exception_survives_retry_wrapper_traceback() -> None:
    """Regression: masking an exception raised through the retry-wrapper traceback
    shape must not raise (previously RuntimeError: dictionary keys changed during
    iteration)."""
    try:
        _FakeRetryingConnection().execute_query("SELECT please_crash")
    except ValueError as e:
        masked = _mask_exception(e)
        assert masked is e


def _value_error_containing(secret: str) -> ValueError:
    # Raise and catch inside this throwaway helper so the returned exception's
    # traceback contains only this frame, not the caller's. On Python 3.13+
    # ``frame.f_locals`` is a write-through proxy (PEP 667), so masking a frame's
    # locals rewrites the live variables; keeping the raise here means the caller's
    # assertion variables can't be clobbered by the masking under test.
    try:
        raise ValueError(f"connection failed for {secret}")
    except ValueError as e:
        return e


def _error_record_with_exception(message: str, exception: BaseException) -> logging.LogRecord:
    return logging.LogRecord(
        name="soda_core.test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=0,
        msg=message,
        args=(),
        exc_info=(type(exception), exception, exception.__traceback__),
    )


@pytest.fixture
def registered_secret():
    secret = "s3cr3t-token-value"
    _masked_values.add(secret)
    try:
        yield secret
    finally:
        _masked_values.discard(secret)


def test_mask_exception_masks_registered_secret_in_args(registered_secret: str) -> None:
    """Masking still redacts registered secrets in the exception args."""
    masked = _mask_exception(_value_error_containing(registered_secret))
    assert masked is not None
    assert registered_secret not in masked.args[0]
    assert "***" in masked.args[0]


def test_mask_exception_returns_none_for_none() -> None:
    assert _mask_exception(None) is None


def test_mask_exception_raises_masking_error_when_masking_fails(monkeypatch, registered_secret: str) -> None:
    """A failure while masking must surface as MaskingError — never be swallowed, and
    never leak the original error (whose frames are exactly what could not be masked)."""

    def _broken_mask_message(message):
        raise RuntimeError("dictionary keys changed during iteration")

    monkeypatch.setattr(logging_configuration, "_mask_message", _broken_mask_message)

    with pytest.raises(MaskingError) as exc_info:
        _mask_exception(_value_error_containing(registered_secret))
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True
    assert registered_secret not in str(exc_info.value)


def test_mask_record_keeps_masked_exception_when_masking_succeeds(registered_secret: str) -> None:
    record = _error_record_with_exception(
        f"query failed for {registered_secret}", _value_error_containing(registered_secret)
    )

    _mask_record(record)

    assert record.exc_info is not None
    exc_type, exc_value, _ = record.exc_info
    assert exc_type is ValueError
    assert registered_secret not in exc_value.args[0]
    assert registered_secret not in record.getMessage()
    assert REDACTED_EXCEPTION_MARKER not in record.getMessage()


def test_mask_record_redacts_exception_when_masking_fails(monkeypatch, registered_secret: str) -> None:
    """If the exception cannot be masked it must not be logged at all: the record keeps
    its (masked) message and ERROR level, but the exception and traceback are dropped and
    the message says so."""

    def _broken_mask_message(message):
        raise RuntimeError("dictionary keys changed during iteration")

    monkeypatch.setattr(logging_configuration, "_mask_message", _broken_mask_message)
    record = _error_record_with_exception(
        f"query failed for {registered_secret}", _value_error_containing(registered_secret)
    )

    _mask_record(record)

    assert record.levelno == logging.ERROR
    assert record.exc_info is None
    assert record.exc_text is None
    assert REDACTED_EXCEPTION_MARKER in record.getMessage()
    assert registered_secret not in record.getMessage()


def test_console_formatter_output_has_no_secret_when_masking_fails(monkeypatch, registered_secret: str) -> None:
    """End-to-end through the console formatter: no secret and no traceback in the output."""

    def _broken_mask_message(message):
        raise RuntimeError("dictionary keys changed during iteration")

    monkeypatch.setattr(logging_configuration, "_mask_message", _broken_mask_message)
    record = _error_record_with_exception(
        f"query failed for {registered_secret}", _value_error_containing(registered_secret)
    )

    rendered = SodaConsoleFormatter().format(record)

    assert registered_secret not in rendered
    assert "Traceback" not in rendered
    assert "ValueError" not in rendered
    assert REDACTED_EXCEPTION_MARKER in rendered


# --- Policy redaction ---------------------------------------------------------------------


def _record(message: str, *args, level: int = logging.INFO, **extra) -> logging.LogRecord:
    """A record the way ``Logger.makeRecord`` builds one: ``extra`` keys become attributes."""
    record = logging.LogRecord(
        name="soda_core.test", level=level, pathname=__file__, lineno=0, msg=message, args=args, exc_info=None
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


class _MustNotBeFormatted:
    def __str__(self) -> str:
        raise AssertionError("a redacted record's args must never be formatted")


GROUP_VALUES = RedactionCategory.GROUP_VALUES
MARKER = redacted_marker(GROUP_VALUES)


@pytest.fixture
def policy_on():
    with redaction_policy({GROUP_VALUES}):
        yield


def test_marker_names_the_category() -> None:
    assert redacted_marker("group_values") == "[redacted: group_values]"


def test_marked_record_redacted_under_policy(policy_on) -> None:
    record = _record("group ['SENTINEL']: 1", redact=GROUP_VALUES)

    _mask_record(record)

    assert record.getMessage() == MARKER
    assert record.args == ()


def test_redaction_does_not_format_args(policy_on) -> None:
    """The trigger is checked before the message is built: values passed as ``args`` are
    never interpolated, and a broken format can't raise ahead of the redaction."""
    record = _record("group %s: %s", _MustNotBeFormatted(), 1, redact=GROUP_VALUES)

    _mask_record(record)

    assert record.getMessage() == MARKER


def test_scope_record_redacted_under_policy(policy_on) -> None:
    with redacted_scope(GROUP_VALUES):
        inside = _record("row SENTINEL | 1")
        _mask_record(inside)
    outside = _record("row SENTINEL | 1")
    _mask_record(outside)

    assert inside.getMessage() == MARKER
    assert outside.getMessage() == "row SENTINEL | 1"


def test_nested_scope_restores_outer(policy_on) -> None:
    with redacted_scope(GROUP_VALUES):
        with redacted_scope(GROUP_VALUES):
            pass
        after_inner = _record("SENTINEL")
        _mask_record(after_inner)
    after_outer = _record("SENTINEL")
    _mask_record(after_outer)

    assert after_inner.getMessage() == MARKER
    assert after_outer.getMessage() == "SENTINEL"


def test_scope_reset_on_exception(policy_on) -> None:
    with pytest.raises(RuntimeError):
        with redacted_scope(GROUP_VALUES):
            raise RuntimeError("query failed")
    record = _record("SENTINEL")
    _mask_record(record)

    assert record.getMessage() == "SENTINEL"


def test_marked_record_unchanged_without_policy() -> None:
    """Sites always mark; without a policy for the category the record is left as is."""
    record = _error_record_with_exception("group SENTINEL failed", _value_error_containing("SENTINEL"))
    record.redact = GROUP_VALUES

    _mask_record(record)

    assert record.getMessage() == "group SENTINEL failed"
    assert record.exc_info is not None
    assert not hasattr(record, "exception")


def test_scope_record_unchanged_without_policy() -> None:
    with redacted_scope(GROUP_VALUES):
        record = _record("row SENTINEL | 1")
        _mask_record(record)

    assert record.getMessage() == "row SENTINEL | 1"


def test_policy_for_other_category_does_not_redact() -> None:
    with redaction_policy({"something_else"}):
        record = _record("row SENTINEL | 1", redact=GROUP_VALUES)
        _mask_record(record)

    assert record.getMessage() == "row SENTINEL | 1"


def test_policy_reset_on_exit() -> None:
    with redaction_policy({GROUP_VALUES}):
        pass
    record = _record("row SENTINEL | 1", redact=GROUP_VALUES)
    _mask_record(record)

    assert record.getMessage() == "row SENTINEL | 1"


def test_inner_policy_does_not_lift_outer() -> None:
    with redaction_policy({GROUP_VALUES}):
        with redaction_policy(()):
            record = _record("row SENTINEL | 1", redact=GROUP_VALUES)
            _mask_record(record)

    assert record.getMessage() == MARKER


def test_empty_policy_is_a_no_op() -> None:
    with redaction_policy(()):
        record = _record("row SENTINEL | 1", redact=GROUP_VALUES)
        _mask_record(record)

    assert record.getMessage() == "row SENTINEL | 1"


def test_is_redacted_follows_policy() -> None:
    assert not is_redacted(GROUP_VALUES)
    with redaction_policy({GROUP_VALUES}):
        assert is_redacted(GROUP_VALUES)
    assert not is_redacted(GROUP_VALUES)


def test_is_redacted_false_for_other_category() -> None:
    with redaction_policy({"something_else"}):
        assert not is_redacted(GROUP_VALUES)


def test_is_redacted_keeps_outer_policy() -> None:
    """A run's own empty policy does not lift what a caller's outer policy redacts."""
    with redaction_policy({GROUP_VALUES}):
        with redaction_policy(()):
            assert is_redacted(GROUP_VALUES)


def test_stamped_record_stays_redacted_outside_scope_and_policy() -> None:
    """A record is masked by every handler it reaches, and a captured record is replayed
    later outside the scope (gatherer switch, forwarding collector). The first pass stamps
    it so later passes keep it redacted, even where no policy is active."""
    with redaction_policy({GROUP_VALUES}):
        with redacted_scope(GROUP_VALUES):
            record = _record("row SENTINEL | 1")
            _mask_record(record)
    assert record.redact == GROUP_VALUES

    record.msg = "row SENTINEL | 1"  # what a replay would see if the stamp were missing
    _mask_record(record)

    assert record.getMessage() == MARKER


def test_redaction_drops_exception(policy_on) -> None:
    """Console: no traceback. Cloud: the ``exception`` extra is the only exception field
    the upload carries; it keeps the exception class (a static identifier, never a value)
    and nothing else."""
    record = _error_record_with_exception("query failed", _value_error_containing("SENTINEL"))
    record.redact = GROUP_VALUES

    _mask_record(record)

    assert record.exc_info is None
    assert record.exc_text is None
    assert record.exception == "ValueError"
    assert record.getMessage() == MARKER


def test_redaction_clears_exception_extra(policy_on) -> None:
    """A stacktrace attached via the ``exception`` extra (without ``exc_info``) is what
    reaches Cloud; it is dropped like the traceback."""
    record = _record("insert failed", redact=GROUP_VALUES, exception="Traceback ... VALUES ('SENTINEL')")

    _mask_record(record)

    assert record.exception is None
    assert record.getMessage() == MARKER


def test_redaction_is_idempotent(policy_on) -> None:
    """A second pass (next handler) keeps the first pass's result, incl. the exception class."""
    record = _error_record_with_exception("query failed", _value_error_containing("SENTINEL"))
    record.redact = GROUP_VALUES

    _mask_record(record)
    _mask_record(record)

    assert record.exception == "ValueError"
    assert record.getMessage() == MARKER


def test_console_formatter_redacts_scope_record(policy_on) -> None:
    record = _error_record_with_exception("query failed for SENTINEL", _value_error_containing("SENTINEL"))

    with redacted_scope(GROUP_VALUES):
        rendered = SodaConsoleFormatter().format(record)

    assert MARKER in rendered
    assert "SENTINEL" not in rendered
    assert "Traceback" not in rendered


def test_cloud_log_dict_of_redacted_record_has_no_values(policy_on) -> None:
    """The two value-bearing fields of a Cloud log dict are ``message`` and ``exception``."""
    from soda_core.common.soda_cloud import build_log_cloud_json_dict

    record = _error_record_with_exception("query failed for SENTINEL", _value_error_containing("SENTINEL"))
    with redacted_scope(GROUP_VALUES):
        _mask_record(record)

    cloud_dict = build_log_cloud_json_dict(record, 0)

    assert cloud_dict["message"] == MARKER
    assert cloud_dict["exception"] == "ValueError"
    assert "SENTINEL" not in str(cloud_dict)


def test_secret_masking_unchanged(registered_secret: str, policy_on) -> None:
    record = _record(f"connecting with {registered_secret}")

    _mask_record(record)

    assert record.getMessage() == "connecting with ***"
    assert MARKER not in record.getMessage()
