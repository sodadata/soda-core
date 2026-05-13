"""Pytest plugin that surfaces snapshot fallback events in test output.

When ``SODA_TEST_SNAPSHOT=replay`` + ``SODA_TEST_SNAPSHOT_FALLBACK=true`` is
active, a test whose recorded SQL no longer matches the production SQL falls
back to the real DB. Two models coexist:

* **Legacy partial fallback** (default): the wrapper continues the test in a
  partial-replay + live-tail hybrid. Visibility provided by yellow
  ``FALLBACK: PASSED`` / ``FALLBACK: FAILED`` markers + an end-of-session
  summary so the SQL drift doesn't get silently masked.
* **Rerun model** (``SODA_TEST_SNAPSHOT_RERUN=true``): a replay error bubbles
  out of the first attempt; the plugin re-runs the test end-to-end against
  the real DB and reports ``RERAN: PASSED`` / ``RERAN: FAILED`` instead. A
  fresh snapshot is saved only on a green rerun AND with
  ``SODA_TEST_SNAPSHOT_RERECORD=true``.

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
    """Reset the per-process fallback registry at session start.

    Stale entries can leak across pytest invocations when running under
    ``pytest --pdb-trace`` style workflows that reuse the interpreter. Clearing
    on configure makes each session start with a clean slate.
    """
    from helpers.snapshot_connection import (
        reset_fallback_test_record,
        reset_pending_rerun_record,
    )

    reset_fallback_test_record()
    reset_pending_rerun_record()


# ---------------------------------------------------------------------------
# Rerun model: pytest_runtest_protocol
# ---------------------------------------------------------------------------


# Process-local record of tests that triggered a re-run, mapping test_id →
# reason. Mirrors _FALLBACK_TEST_RECORD in shape so reporting paths can use the
# same machinery. Populated by ``pytest_runtest_protocol`` after a successful
# re-run; ``pytest_report_teststatus`` reads it to label the test.
_RERAN_TEST_RECORD: dict[str, str] = {}


def _rerun_mode_enabled() -> bool:
    from helpers.snapshot_connection import is_rerun_mode_enabled

    return is_rerun_mode_enabled()


def _reset_per_test_rerun_state() -> None:
    """Clear the pending-rerun queue before each test attempt."""
    from helpers.snapshot_connection import reset_pending_rerun_record

    reset_pending_rerun_record()


def _pop_rerun_reason(test_id: str) -> Optional[str]:
    """Pop and return the rerun reason for ``test_id`` if any was queued."""
    from helpers.snapshot_connection import get_pending_rerun_record

    return get_pending_rerun_record().pop(test_id, None)


def _mark_all_wrappers_for_rerun(test_id: str, *, record: bool) -> None:
    """Switch every live snapshot wrapper into passthrough for ``test_id``.

    Walks the process-wide weak set populated by
    ``SnapshotDataSourceConnection.__init__``. Captures primary, DWH, and
    any reconciliation secondaries in one pass without needing explicit
    bookkeeping per test type.
    """
    from helpers.snapshot_connection import get_live_snapshot_wrappers

    for wrapper in list(get_live_snapshot_wrappers()):
        try:
            wrapper.mark_test_for_rerun(test_id, record=record)
        except Exception as exc:
            logger.warning(f"SNAPSHOT: failed to mark wrapper for rerun ({test_id}): {exc!r}")


def _finalize_all_wrappers_rerun(test_id: str, *, success: bool) -> None:
    """Save (on green) / discard rerun recording and clear passthrough flags."""
    from helpers.snapshot_connection import get_live_snapshot_wrappers

    for wrapper in list(get_live_snapshot_wrappers()):
        try:
            wrapper.finalize_test_rerun(test_id, success=success)
        except Exception as exc:
            logger.warning(f"SNAPSHOT: failed to finalize rerun for wrapper ({test_id}): {exc!r}")


def _deactivate_lingering_dwh_interceptor() -> None:
    """If a DWH interceptor outlived the failed attempt, deactivate it.

    soda-extensions ships ``DwhSnapshotInterceptor``; its ``_active_instance``
    class attribute points at any live interceptor. We deactivate before the
    re-run so the next attempt re-installs cleanly. Import is lazy + best-
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
    """If any report indicates the test asked for a re-run, return the reason.

    A re-run is triggered when:
      - the test's call phase raised a SnapshotReplayError (the wrapper
        populated _PENDING_RERUN before re-raising), AND
      - setup + teardown phases all succeeded (per user requirement: the
        whole lifecycle must be valid for a re-run to make sense).
    """
    # Setup or teardown failures: no re-run.
    for r in reports:
        if r.when in ("setup", "teardown") and r.failed:
            return None
    return _pop_rerun_reason(test_id)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item, nextitem):  # noqa: D401
    """Run the test once. If it signalled a rerun, run it once more against real DB.

    Only intercepts when ``SODA_TEST_SNAPSHOT_RERUN=true``. Returns ``True``
    to tell pytest "I handled this test". The hook is opt-in to keep the
    legacy fallback path unaffected for callers that haven't migrated.

    A single re-run is the maximum: if the rerun itself raises another
    SnapshotReplayError, that bubbles up as a hard test failure (per the spec
    decision — "single rerun, if that fails, error out").
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

    # First attempt asked for a re-run. Drain any straggler reports without
    # publishing them (the test outcome will come from the second attempt).
    logger.info(f"SNAPSHOT: re-running {item.nodeid} against real DB; reason: {rerun_reason}")
    import os as _os

    from helpers.snapshot_connection import (
        begin_rerun_in_progress,
        end_rerun_in_progress,
    )

    rerecord = _os.getenv("SODA_TEST_SNAPSHOT_RERECORD", "").lower() == "true"
    _deactivate_lingering_dwh_interceptor()
    _mark_all_wrappers_for_rerun(item.nodeid, record=rerecord)
    # Pre-arm any wrappers constructed *during* the rerun (DWH interceptor's
    # SnapshotDataSourceConnection, recon secondaries) so their first SQL
    # op routes to the real DB without needing a second mark-pass.
    begin_rerun_in_progress(item.nodeid, record=rerecord)
    _reset_per_test_rerun_state()  # clear queue before the second attempt

    try:
        second_reports = runtestprotocol(item, nextitem=nextitem, log=False)
    finally:
        end_rerun_in_progress(item.nodeid)
    # If the rerun also signalled another rerun, that's a hard failure —
    # the user asked for at most one re-run.
    leftover = _pop_rerun_reason(item.nodeid)
    if leftover is not None:
        logger.error(
            f"SNAPSHOT: re-run of {item.nodeid} also produced a replay error ({leftover}); "
            "not retrying again. Surfacing the second attempt's outcome as the test result."
        )

    rerun_succeeded = all(r.passed for r in second_reports if r.when == "call") and not any(
        r.failed for r in second_reports
    )
    _finalize_all_wrappers_rerun(item.nodeid, success=rerun_succeeded)

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


def pytest_runtest_makereport(item, call) -> None:
    """Stamp the test report with the fallback reason (if any) for the call phase.

    Reading the registry here — once per test — lets later hooks
    (``pytest_report_teststatus``, ``pytest_terminal_summary``) work off the
    report object without re-querying global state. Avoids races and works
    naturally under pytest-xdist, where each worker has its own registry but
    reports are forwarded to the master.
    """
    if call.when != "call":
        return
    from helpers.snapshot_connection import get_fallback_test_record

    reason: Optional[str] = get_fallback_test_record().get(item.nodeid)
    if reason is not None:
        # Stash on the report so other hooks can read without touching globals.
        # Using a setattr rather than user_properties to avoid leaking the
        # marker into JUnit XML (which user_properties does).
        item.stash.setdefault(_FALLBACK_STASH_KEY, reason)


def pytest_report_teststatus(report, config):  # noqa: D401
    """Annotate the call-phase status for tests that re-ran or fell back.

    Two paths share this hook because their reporting intent is the same:
    surface the unusual state in the per-test status line while preserving
    pytest's pass/fail accounting for the exit code and summary counts.

    The short progress char stays a plain ASCII string. Pytest concatenates
    those into a buffer and later calls ``.rsplit`` on the result, so a
    markup tuple here triggers ``AttributeError: 'tuple' object has no
    attribute 'rsplit'`` deep inside pytest's terminal printer. The verbose
    label is safe to colourize because it's only handed to the markup-aware
    write path.
    """
    if report.when != "call":
        return None
    # Rerun model takes precedence — if the test was reran, the partial-
    # fallback registry would never have been populated anyway. Source the
    # reason from user_properties (propagated through xdist) and fall back
    # to the master-process registry for non-xdist runs.
    reran_reason = _read_reran_reason_from_report(report)
    if reran_reason is not None:
        _RERAN_TEST_RECORD.setdefault(report.nodeid, reran_reason)
        if report.outcome == "passed":
            return "passed", ".", _yellow("RERAN: PASSED")
        if report.outcome == "failed":
            return "failed", "F", _yellow("RERAN: FAILED")
        return None
    reason = _read_report_fallback_reason(report)
    if reason is None:
        return None
    # KEEP the original category ("passed"/"failed") so the exit code and
    # summary counts remain unchanged — a failing test that happened to also
    # fall back must still be counted as failed.
    if report.outcome == "passed":
        return "passed", ".", _yellow("FALLBACK: PASSED")
    if report.outcome == "failed":
        return "failed", "F", _yellow("FALLBACK: FAILED")
    # Other outcomes (skipped, error) shouldn't normally occur with fallback,
    # but if they do, leave them to pytest's defaults rather than risk
    # masking the real signal.
    return None


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:  # noqa: D401
    """Print clear sections listing tests that fell back or were re-run."""
    from helpers.snapshot_connection import get_fallback_test_record

    if _RERAN_TEST_RECORD:
        terminalreporter.section("snapshot drift — tests re-run against real DB", sep="=", yellow=True)
        for test_id in sorted(_RERAN_TEST_RECORD):
            terminalreporter.write_line(f"  {test_id}")
            for line in str(_RERAN_TEST_RECORD[test_id]).splitlines():
                terminalreporter.write_line(f"      {line}")
        terminalreporter.write_line("")
        terminalreporter.write_line(
            f"{len(_RERAN_TEST_RECORD)} test(s) re-ran end-to-end against the real DB "
            "because their recorded SQL no longer matches production. Re-record with "
            "SODA_TEST_SNAPSHOT=record (or set SODA_TEST_SNAPSHOT_RERECORD=true on a "
            "rerun-mode replay run) to refresh the snapshots."
        )

    record = get_fallback_test_record()
    if not record:
        return
    terminalreporter.section("snapshot fallback triggered", sep="=", yellow=True)
    for test_id in sorted(record):
        terminalreporter.write_line(f"  {test_id}")
        # Indent the (potentially multi-line) reason for readability.
        for line in str(record[test_id]).splitlines():
            terminalreporter.write_line(f"      {line}")
    terminalreporter.write_line("")
    terminalreporter.write_line(
        f"{len(record)} test(s) triggered snapshot fallback. The recorded SQL "
        "no longer matches what production emits; re-record with "
        "SODA_TEST_SNAPSHOT=record (or set SODA_TEST_SNAPSHOT_RERECORD=true on "
        "a fallback-enabled replay run) to bring the snapshots up to date."
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


# Pytest's stash keys are typed; we use a plain attribute since we only read
# back through the same module. A constant string is fine.
_FALLBACK_STASH_KEY = "_soda_snapshot_fallback_reason"


def _read_report_fallback_reason(report) -> Optional[str]:
    """Pull the fallback reason off the test item's stash via the report.

    ``report.item`` is not always available depending on pytest version, so we
    walk through the nodeid-keyed registry as a fallback. Both routes give the
    same answer in practice; the stash route is just slightly cheaper.
    """
    from helpers.snapshot_connection import get_fallback_test_record

    return get_fallback_test_record().get(report.nodeid)


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
