from __future__ import annotations

from soda_core.common.logging_configuration import _mask_exception, _masked_values


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


def test_mask_exception_masks_registered_secret_in_args() -> None:
    """Masking still redacts registered secrets in the exception args."""
    secret = "s3cr3t-token-value"
    _masked_values.add(secret)
    try:
        try:
            raise ValueError(f"connection failed for {secret}")
        except ValueError as e:
            masked = _mask_exception(e)
        assert masked is not None
        assert secret not in masked.args[0]
        assert "***" in masked.args[0]
    finally:
        _masked_values.discard(secret)


def test_mask_exception_returns_none_for_none() -> None:
    assert _mask_exception(None) is None
