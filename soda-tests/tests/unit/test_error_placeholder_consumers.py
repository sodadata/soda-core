"""Pin downstream-safe shape of the ERROR-status ``CheckCollectionResult``
placeholder against common consumer code paths.

The session impl builds an ERROR placeholder via
``CheckCollectionResult.error_placeholder(...)`` whenever a per-spec
verification fails before producing a real result. Consumers (backend
ingestion, CLI loops, library glue) iterate ``check_collection_results``
without per-slot ``None`` guards — the design doc requires the placeholder
to be a real ``CheckCollectionResult`` instance with safe defaults.

This smoke test exercises the read paths consumers most commonly hit:

* ``result.contract`` — populated from the placeholder factory's ``contract``
  arg (never None).
* ``result.data_source`` — ``None`` on the placeholder.
* ``result.check_results`` — empty list (iteration safe).
* ``result.log_records`` — non-empty (contains the synthesized error
  message so ``get_errors()`` returns useful output).
* ``result.has_errors`` — ``True`` because ``status is ERROR``.
* ``CheckCollectionSessionResult([error_result]).get_errors()`` —
  aggregates the placeholder's error message into the session's error
  list.

If any of these read paths crashes, the placeholder factory needs safer
defaults (or the consumer needs ``None`` guards). The bar this test pins:
"downstream-safe shape" — the placeholder behaves like a real result for
all standard read paths.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
    CheckCollectionSessionResult,
    CheckCollectionStatus,
    CheckCollectionTarget,
    YamlFileContentInfo,
)


def _error_placeholder() -> CheckCollectionResult:
    """Build a typical ERROR placeholder the way the session impl would."""
    target = CheckCollectionTarget(
        data_source_name=None,
        dataset_prefix=[],
        dataset_name="",
        soda_qualified_dataset_name="",
        source=YamlFileContentInfo(source_content_str=None, local_file_path=None),
        wire_source="soda-sentinel",
    )
    # Synthesize a LogRecord the same shape ``_build_error_result`` builds.
    log_record = logging.getLogger("soda_core.test").makeRecord(
        name="soda_core.test",
        level=logging.ERROR,
        fn=__file__,
        lno=1,
        msg="simulated error message for placeholder consumer test",
        args=None,
        exc_info=None,
    )
    return CheckCollectionResult.error_placeholder(
        contract=target,
        log_record=log_record,
        originating_exception=RuntimeError("simulated"),
        started_timestamp=datetime.now(tz=timezone.utc),
        ended_timestamp=datetime.now(tz=timezone.utc),
    )


def test_placeholder_contract_attribute_is_set_and_safe_to_read():
    """``result.contract`` is the ``CheckCollectionTarget`` passed into the
    factory — never ``None``. Consumers reading ``result.contract.dataset_name``
    etc. don't need a None guard.
    """
    result = _error_placeholder()
    assert isinstance(result.contract, CheckCollectionTarget)
    # Reading nested attrs must not raise — pins the typed-attribute contract.
    assert result.contract.wire_source == "soda-sentinel"
    assert result.contract.dataset_name == ""


def test_placeholder_data_source_is_none_and_safe_to_check():
    """``result.data_source`` is ``None`` on the placeholder. Consumers
    typically branch on ``if result.data_source is not None`` rather than
    reading attributes off it — pin the documented None default.
    """
    result = _error_placeholder()
    assert result.data_source is None


def test_placeholder_check_results_is_empty_list_and_safe_to_iterate():
    """``result.check_results`` is an empty list (not None) so consumers
    iterating without a None guard don't crash.
    """
    result = _error_placeholder()
    assert result.check_results == []
    # Iteration must not raise.
    assert list(result.check_results) == []
    # Number-of-checks aggregations on the empty list return 0.
    assert result.number_of_checks == 0
    assert result.number_of_checks_passed == 0
    assert result.number_of_checks_failed == 0
    assert result.number_of_checks_excluded == 0


def test_placeholder_log_records_contain_error_message():
    """``result.log_records`` is non-empty — the placeholder synthesizes a
    single ERROR-level record so ``get_errors()`` returns the error message
    rather than an empty list.
    """
    result = _error_placeholder()
    assert result.log_records is not None
    assert len(result.log_records) >= 1
    errors = result.get_errors()
    assert any(
        "simulated error message" in msg for msg in errors
    ), f"placeholder did not surface its error message via get_errors(): {errors}"


def test_placeholder_has_errors_is_true():
    """``result.has_errors`` returns ``True`` because ``status is ERROR``.
    Consumers checking ``result.has_errors`` (e.g. CLI exit-code logic)
    see the placeholder as a failure.
    """
    result = _error_placeholder()
    assert result.status is CheckCollectionStatus.ERROR
    assert result.has_errors is True
    # The placeholder is not "ok" — ``is_ok`` aggregates ``not has_errors``.
    assert result.is_ok is False


def test_session_result_aggregates_placeholder_errors():
    """``CheckCollectionSessionResult([error_result]).get_errors()`` aggregates
    the placeholder's error message into the session-level error list.
    Consumers reporting session-wide failures rely on this aggregation.
    """
    placeholder = _error_placeholder()
    session_result = CheckCollectionSessionResult(check_collection_results=[placeholder])
    aggregated_errors = session_result.get_errors()
    assert aggregated_errors  # non-empty
    assert any("simulated error message" in msg for msg in aggregated_errors)
    # has_errors aggregates across results.
    assert session_result.has_errors is True
    # Number-of-checks aggregations on a session containing only a placeholder return 0.
    assert session_result.number_of_checks == 0
    assert session_result.number_of_checks_passed == 0
    assert session_result.number_of_checks_failed == 0
