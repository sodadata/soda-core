"""Pytest plugin that powers the rerun-on-mismatch model for snapshot tests.

When ``SODA_TEST_SNAPSHOT=replay`` + ``SODA_TEST_SNAPSHOT_FALLBACK=true`` is
active, a test whose recorded SQL no longer matches what production now emits
raises a ``SnapshotReplayError`` out of the first attempt. This plugin's
``pytest_runtest_protocol`` hook then runs the test a second time with every
snapshot wrapper swapped out for its real ``DataSourceConnection`` — so the
rerun behaves identically to ``SODA_TEST_SNAPSHOT=off``. Tests that pass on
the rerun are reported as ``RERAN: PASSED``; failures as ``RERAN: FAILED``.

Strict mode (``SODA_TEST_SNAPSHOT_STRICT=true``) disables the rerun and lets
the first attempt's failure stand. Used for nightly post-record verification
to surface any non-determinism in the recorded SQL.

Registered via ``pytest_plugins = ["helpers.snapshot_pytest_plugin"]`` in each
repo's ``conftest.py`` so it works for soda-core, soda-extensions, and any
downstream repo that consumes soda-tests.
"""

from __future__ import annotations

import logging
from typing import Optional

import pytest

logger = logging.getLogger(__name__)


def pytest_configure(config) -> None:  # noqa: D401
    """Reset the per-process pending-rerun registry at session start.

    Stale entries can leak across pytest invocations when running under
    ``pytest --pdb-trace`` style workflows that reuse the interpreter. Clearing
    on configure makes each session start with a clean slate.
    """
    from helpers.snapshot_connection import reset_pending_rerun_record

    reset_pending_rerun_record()


# ---------------------------------------------------------------------------
# Rerun model: pytest_runtest_protocol
# ---------------------------------------------------------------------------


# Process-local record of tests that triggered a rerun, mapping test_id → reason.
# Populated by ``pytest_runtest_protocol`` after a successful rerun;
# ``pytest_report_teststatus`` reads it to label the test.
_RERAN_TEST_RECORD: dict[str, str] = {}


def _rerun_mode_enabled() -> bool:
    """Whether the plugin should run the rerun hook.

    False when strict mode is active — then pytest runs each test once and
    surfaces any ``SnapshotReplayError`` as a hard failure.
    """
    from helpers.snapshot_connection import is_strict_mode_enabled

    return not is_strict_mode_enabled()


def _reset_per_test_rerun_state() -> None:
    """Clear the pending-rerun queue before each test attempt."""
    from helpers.snapshot_connection import reset_pending_rerun_record

    reset_pending_rerun_record()


def _pop_rerun_reason(test_id: str) -> Optional[str]:
    """Pop and return the rerun reason for ``test_id`` if any was queued."""
    from helpers.snapshot_connection import get_pending_rerun_record

    return get_pending_rerun_record().pop(test_id, None)


def _swap_all_wrappers_to_real(test_id: str) -> list:
    """Detach every live snapshot wrapper from its DataSourceImpl.

    After this call, the DataSourceImpl's ``data_source_connection`` points
    at a real ``DataSourceConnection`` directly — no wrapper in the call
    chain. The rerun's test body runs identically to off-mode.

    Returns the list of wrappers that were successfully swapped so the
    plugin can restore them in a ``finally`` block.
    """
    from helpers.snapshot_connection import get_live_snapshot_wrappers

    swapped: list = []
    for wrapper in list(get_live_snapshot_wrappers()):
        try:
            if wrapper.swap_to_real_for_rerun():
                swapped.append(wrapper)
        except Exception as exc:
            logger.warning(f"SNAPSHOT: failed to swap wrapper to real for rerun ({test_id}): {exc!r}")
    return swapped


def _restore_all_swapped_wrappers(swapped: list) -> None:
    """Re-attach previously-swapped wrappers to their DataSourceImpls."""
    for wrapper in swapped:
        try:
            wrapper.restore_from_rerun()
        except Exception as exc:
            logger.warning(f"SNAPSHOT: failed to restore wrapper after rerun: {exc!r}")


def _deactivate_lingering_dwh_interceptor() -> None:
    """If a DWH interceptor outlived the failed attempt, deactivate it.

    soda-extensions ships ``DwhSnapshotInterceptor``; its ``_active_instance``
    class attribute points at any live interceptor. We deactivate before the
    rerun so the next attempt re-installs cleanly. Import is lazy + best-
    effort so soda-core (which has no DWH module) doesn't break.
    """
    try:
        from test_helpers.dwh_setup import DwhSnapshotInterceptor  # type: ignore
    except Exception:
        return
    inst = getattr(DwhSnapshotInterceptor, "_active_instance", None)
    if inst is not None:
        try:
            inst.deactivate()
        except Exception as exc:
            logger.warning(f"SNAPSHOT: failed to deactivate lingering DWH interceptor: {exc!r}")


def _reports_contain_rerun_signal(reports: list, test_id: str) -> Optional[str]:
    """If any report indicates the test asked for a rerun, return the reason.

    A rerun is triggered when:
      - the test's call phase raised a SnapshotReplayError (the wrapper
        populated _PENDING_RERUN before re-raising), AND
      - setup + teardown phases all succeeded (per user requirement: the
        whole lifecycle must be valid for a rerun to make sense).
    """
    # Setup or teardown failures: no rerun.
    for r in reports:
        if r.when in ("setup", "teardown") and r.failed:
            return None
    return _pop_rerun_reason(test_id)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item, nextitem):  # noqa: D401
    """Run the test once. If it signalled a rerun, run it once more against real DB.

    Skips entirely in strict mode so the first attempt's failure stands.
    Returns ``True`` to tell pytest "I handled this test". A single rerun
    is the maximum: if the rerun itself raises another SnapshotReplayError,
    that bubbles up as a hard test failure.
    """
    if not _rerun_mode_enabled():
        return None
    from _pytest.runner import runtestprotocol

    _reset_per_test_rerun_state()

    first_reports = runtestprotocol(item, nextitem=nextitem, log=False)
    rerun_reason = _reports_contain_rerun_signal(first_reports, item.nodeid)
    if rerun_reason is None:
        for r in first_reports:
            item.ihook.pytest_runtest_logreport(report=r)
        return True

    # First attempt asked for a rerun. Run the test again with all
    # snapshot wrappers swapped out for their real connections, so the
    # rerun's behaviour is identical to SODA_TEST_SNAPSHOT=off.
    logger.warning(f"SNAPSHOT: re-running {item.nodeid} against real DB; reason: {rerun_reason}")

    from helpers.snapshot_connection import (
        begin_rerun_in_progress,
        end_rerun_in_progress,
    )

    _deactivate_lingering_dwh_interceptor()
    # Track the rerun BEFORE swapping so that wrappers built mid-rerun
    # (DwhSnapshotInterceptor's wrapper, recon secondaries) take the
    # bypass path and don't wrap anything.
    begin_rerun_in_progress(item.nodeid)
    swapped = _swap_all_wrappers_to_real(item.nodeid)
    _reset_per_test_rerun_state()  # clear queue before the second attempt

    try:
        second_reports = runtestprotocol(item, nextitem=nextitem, log=False)
    finally:
        _restore_all_swapped_wrappers(swapped)
        end_rerun_in_progress(item.nodeid)
    # If the rerun also signalled another rerun, that's a hard failure —
    # the user asked for at most one rerun.
    leftover = _pop_rerun_reason(item.nodeid)
    if leftover is not None:
        logger.error(
            f"SNAPSHOT: re-run of {item.nodeid} also produced a replay error ({leftover}); "
            "not retrying again. Surfacing the second attempt's outcome as the test result."
        )

    rerun_succeeded = all(r.passed for r in second_reports if r.when == "call") and not any(
        r.failed for r in second_reports
    )

    # Stash so pytest_report_teststatus can label the call-phase report as RERAN: …
    _RERAN_TEST_RECORD[item.nodeid] = rerun_reason
    # Also attach to the call-phase report's user_properties so the rerun
    # reason propagates through pytest-xdist to the master (which doesn't
    # share the worker's _RERAN_TEST_RECORD dict).
    for r in second_reports:
        if r.when == "call":
            r.user_properties = list(r.user_properties or []) + [
                (_RERAN_USER_PROPERTY_KEY, rerun_reason),
            ]
    for r in second_reports:
        item.ihook.pytest_runtest_logreport(report=r)
    return True


# Key used to ferry the rerun reason through xdist via report.user_properties.
_RERAN_USER_PROPERTY_KEY = "_soda_snapshot_reran_reason"


def pytest_report_teststatus(report, config):  # noqa: D401
    """Annotate the call-phase status for tests that re-ran.

    The short progress char stays a plain ASCII string. Pytest concatenates
    those into a buffer and later calls ``.rsplit`` on the result, so a
    markup tuple here triggers ``AttributeError: 'tuple' object has no
    attribute 'rsplit'`` deep inside pytest's terminal printer. The verbose
    label is safe to colourize because it's only handed to the markup-aware
    write path.
    """
    if report.when != "call":
        return None
    reran_reason = _read_reran_reason_from_report(report)
    if reran_reason is None:
        return None
    _RERAN_TEST_RECORD.setdefault(report.nodeid, reran_reason)
    if report.outcome == "passed":
        return "passed", ".", _yellow("RERAN: PASSED")
    if report.outcome == "failed":
        return "failed", "F", _yellow("RERAN: FAILED")
    return None


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:  # noqa: D401
    """Print a section listing tests that re-ran against the real DB."""
    from helpers.snapshot_connection import is_strict_mode_enabled

    if is_strict_mode_enabled():
        terminalreporter.write_sep(
            "=",
            "SODA_TEST_SNAPSHOT_STRICT=true — snapshot mismatches fail the test (no rerun)",
            yellow=True,
            bold=True,
        )

    if not _RERAN_TEST_RECORD:
        return
    terminalreporter.section("snapshot drift — tests re-run against real DB", sep="=", yellow=True)
    for test_id in sorted(_RERAN_TEST_RECORD):
        terminalreporter.write_line(f"  {test_id}")
        for line in str(_RERAN_TEST_RECORD[test_id]).splitlines():
            terminalreporter.write_line(f"      {line}")
    terminalreporter.write_line("")
    terminalreporter.write_line(
        f"{len(_RERAN_TEST_RECORD)} test(s) re-ran end-to-end against the real DB "
        "because their recorded SQL no longer matches production. Re-record with "
        "SODA_TEST_SNAPSHOT=record to refresh the snapshots."
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _read_reran_reason_from_report(report) -> Optional[str]:
    """Pull the rerun reason off ``report.user_properties`` if present.

    Falls back to the master-process registry so non-xdist runs still work.
    Under xdist the user_properties are serialised across the worker→master
    boundary, which is why we attach the reason there in
    ``pytest_runtest_protocol``.
    """
    for key, value in report.user_properties or []:
        if key == _RERAN_USER_PROPERTY_KEY:
            return value
    return _RERAN_TEST_RECORD.get(report.nodeid)


def _yellow(text: str) -> tuple:
    """Return the (text, markup) tuple pytest expects for coloured short status.

    Pytest's ``pytest_report_teststatus`` accepts either a plain string or a
    ``(text, {"yellow": True})`` style tuple for the short letter / verbose
    label. Wrapping in a tuple gives us colour without pulling in colorama.
    """
    return (text, {"yellow": True, "bold": True})
