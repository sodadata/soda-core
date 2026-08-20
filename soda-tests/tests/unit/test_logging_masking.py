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
)


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
