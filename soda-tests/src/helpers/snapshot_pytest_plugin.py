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
from helpers.snapshot_connection import (
    begin_rerun_in_progress,
    end_rerun_in_progress,
    get_discarded_unconsumed_record,
    get_live_snapshot_wrappers,
    get_pending_rerun_record,
    is_strict_mode_enabled,
    reset_discarded_unconsumed_record,
    reset_pending_rerun_record,
    set_pytest_plugin_active,
)
from helpers.snapshot_interceptor import deactivate_all_registered_interceptors

logger = logging.getLogger(__name__)


# Key used to ferry the rerun reason through xdist via report.user_properties.
# JUnit XML schema requires attribute names matching [A-Za-z][A-Za-z0-9._-]*,
# so this key cannot start with an underscore.
_RERAN_USER_PROPERTY_KEY = "soda_snapshot_reran_reason"


def pytest_configure() -> None:
    """Reset per-process state at session start and signal plugin presence.

    Stale entries can leak across pytest invocations when running under
    ``pytest --pdb-trace`` style workflows that reuse the interpreter. Clearing
    on configure makes each session start with a clean slate.

    No matching ``pytest_unconfigure`` hook — the active flag is process-wide,
    and our own unit tests spin up nested ``pytester`` sessions that would
    otherwise clear it for the outer process when they tear down.
    """
    reset_pending_rerun_record()
    reset_discarded_unconsumed_record()
    _RERAN_TEST_RECORD.clear()
    set_pytest_plugin_active(True)


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
    return not is_strict_mode_enabled()


def _reset_per_test_rerun_state() -> None:
    """Clear the pending-rerun queue before each test attempt."""
    reset_pending_rerun_record()


def _pop_rerun_reason(test_id: str) -> Optional[str]:
    """Pop and return the rerun reason for ``test_id`` if any was queued."""
    return get_pending_rerun_record().pop(test_id, None)


def _swap_all_wrappers_to_real(test_id: str) -> list:
    """Detach every live snapshot wrapper from its DataSourceImpl.

    After this call, the DataSourceImpl's ``data_source_connection`` points
    at a real ``DataSourceConnection`` directly — no wrapper in the call
    chain. The rerun's test body runs identically to off-mode.

    Returns the list of wrappers that were successfully swapped so the
    plugin can restore them in a ``finally`` block.
    """
    all_wrappers = list(get_live_snapshot_wrappers())
    logger.info(f"SNAPSHOT: rerun swap for {test_id} — {len(all_wrappers)} live wrapper(s) to consider")
    swapped: list = []
    for wrapper in all_wrappers:
        try:
            if wrapper.swap_to_real_for_rerun():
                swapped.append(wrapper)
                logger.info(
                    f"SNAPSHOT: rerun swap SUCCESS for {test_id} — wrapper id={id(wrapper)} "
                    f"detached from data_source_impl; data_source_connection is now real"
                )
            else:
                logger.info(
                    f"SNAPSHOT: rerun swap SKIPPED for {test_id} — wrapper id={id(wrapper)} "
                    f"(secondary, no back-ref, or no real connection available)"
                )
        except Exception as exc:
            logger.warning(f"SNAPSHOT: rerun swap FAILED for {test_id} — wrapper id={id(wrapper)}: {exc!r}")
    logger.info(
        f"SNAPSHOT: rerun swap for {test_id} — {len(swapped)} of {len(all_wrappers)} wrapper(s) swapped to real"
    )
    return swapped


def _restore_all_swapped_wrappers(swapped: list) -> None:
    """Re-attach previously-swapped wrappers to their DataSourceImpls."""
    for wrapper in swapped:
        try:
            wrapper.restore_from_rerun()
        except Exception as exc:
            logger.warning(f"SNAPSHOT: failed to restore wrapper after rerun: {exc!r}")


def _deactivate_lingering_interceptors() -> None:
    """Best-effort: deactivate every registered snapshot interceptor before
    firing the rerun, so no stale wrapping survives into the rerun's test
    body. Walks the process-wide registry maintained by
    ``snapshot_interceptor`` — no reflection on a specific class, so any
    extension that registers itself is handled the same way.
    """
    deactivate_all_registered_interceptors()


def _reports_contain_rerun_signal(test_id: str) -> Optional[str]:
    """If any report indicates the test asked for a rerun, return the reason.

    A rerun is triggered when the wrapper queued an entry in ``_PENDING_RERUN``
    for this test_id. The queue entry takes priority over per-phase pass/fail:
    when a fixture (e.g. ``ensure_test_table``) is the first caller into the
    wrapper for a new test, a missing-snapshot error fires during *setup*,
    not *call*, but it still means "no recording — re-run against the real
    DB". An empty queue + any non-rerun failure (real bug in setup or
    teardown) returns None so the test stands as it ran.
    """
    return _pop_rerun_reason(test_id)


def _is_xdist_controller(config) -> bool:
    """True if running under pytest-xdist on the controller process AND xdist is
    actively distributing (``-n`` / ``--dist`` is set).

    Workers have ``workerinput`` set; the controller doesn't. xdist may be
    installed and registered as a plugin but inert when ``-n`` is not
    specified (``config.option.dist`` is then ``"no"``), in which case the
    controller is just the test runner and our hook should fire normally.
    """
    if hasattr(config, "workerinput"):
        return False
    if not config.pluginmanager.hasplugin("xdist"):
        return False
    dist = getattr(config.option, "dist", "no")
    return dist != "no"


def _runtestprotocol(item, nextitem):
    """Indirection around the pytest runner so the private import is local.

    ``_pytest.runner.runtestprotocol`` is private API. Importing here
    raises a clear error on incompatible pytest versions rather than at
    plugin load time, and keeps the import out of the module's public
    surface.
    """
    try:
        from _pytest.runner import runtestprotocol
    except ImportError as exc:  # pragma: no cover — defensive for future pytest
        raise RuntimeError(
            "snapshot_pytest_plugin requires _pytest.runner.runtestprotocol "
            f"(pytest internals). Pin a compatible pytest version. Original: {exc!r}"
        ) from exc
    return runtestprotocol(item, nextitem=nextitem, log=False)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item, nextitem):
    """Run the test once. If it signalled a rerun, run it once more against real DB.

    When running under pytest-xdist as the controller, return ``None``
    so xdist's own protocol hook dispatches the test to a worker. The
    rerun logic re-runs on the worker, which is where snapshot wrappers
    actually live.

    Skips entirely in strict mode so the first attempt's failure stands.
    A single rerun is the maximum: if the rerun itself raises another
    SnapshotReplayError, that bubbles up as a hard test failure.
    """
    if not _rerun_mode_enabled():
        return None
    if _is_xdist_controller(item.config):
        # Controller only dispatches; the rerun fires on the worker where
        # snapshot wrappers actually live.
        return None

    _reset_per_test_rerun_state()
    first_reports = _runtestprotocol(item, nextitem)
    rerun_reason = _reports_contain_rerun_signal(item.nodeid)
    if rerun_reason is None:
        for r in first_reports:
            item.ihook.pytest_runtest_logreport(report=r)
        return True

    # First attempt asked for a rerun. Run the test again with all
    # snapshot wrappers swapped out for their real connections, so the
    # rerun's behaviour is identical to SODA_TEST_SNAPSHOT=off.
    logger.warning(f"SNAPSHOT: re-running {item.nodeid} against real DB; reason: {rerun_reason}")

    _deactivate_lingering_interceptors()
    swapped: list = []
    # begin_rerun_in_progress and swap both happen inside try/finally so
    # an exception during swap can't leak the global rerun flag.
    try:
        begin_rerun_in_progress(item.nodeid)
        logger.info(f"SNAPSHOT: rerun ENTER — _RERUN_IN_PROGRESS set for {item.nodeid}")
        swapped = _swap_all_wrappers_to_real(item.nodeid)
        _reset_per_test_rerun_state()  # clear queue before the second attempt

        logger.info(f"SNAPSHOT: rerun CALL runtestprotocol(attempt=2) for {item.nodeid}")
        second_reports = _runtestprotocol(item, nextitem)
    finally:
        _restore_all_swapped_wrappers(swapped)
        end_rerun_in_progress(item.nodeid)
        logger.info(f"SNAPSHOT: rerun EXIT — _RERUN_IN_PROGRESS cleared for {item.nodeid}")

    leftover = _pop_rerun_reason(item.nodeid)
    if leftover is not None:
        logger.error(
            f"SNAPSHOT: re-run of {item.nodeid} also produced a replay error ({leftover}); "
            "not retrying again. Surfacing the second attempt's outcome as the test result."
        )

    # Stash so pytest_report_teststatus can label the call-phase report as RERAN: …
    _RERAN_TEST_RECORD[item.nodeid] = rerun_reason
    # Also attach to the call-phase report's user_properties so the rerun
    # reason propagates through pytest-xdist to the master.
    for r in second_reports:
        if r.when == "call":
            r.user_properties = list(r.user_properties or []) + [
                (_RERAN_USER_PROPERTY_KEY, rerun_reason),
            ]
    for r in second_reports:
        item.ihook.pytest_runtest_logreport(report=r)
    return True


def pytest_report_teststatus(report):
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


def pytest_terminal_summary(terminalreporter) -> None:
    """Print sections listing snapshot drift events: reran tests and tests
    whose unconsumed-snapshot errors were silently discarded."""
    if is_strict_mode_enabled():
        terminalreporter.write_sep(
            "=",
            "SODA_TEST_SNAPSHOT_STRICT=true — snapshot mismatches fail the test (no rerun)",
            yellow=True,
            bold=True,
        )

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
            "SODA_TEST_SNAPSHOT=record to refresh the snapshots."
        )

    # Unconsumed-snapshot drifts that were downgraded silently because
    # they were detected after the owning test's reports had already been
    # emitted (the boundary handler fires from the *next* test's first
    # execute_query). The rerun queue cannot reach those tests anymore, so
    # surface them here. Re-recording is the only way to clear them.
    discarded = get_discarded_unconsumed_record()
    if discarded:
        terminalreporter.section(
            "snapshot drift — unconsumed entries discarded (no rerun fired)",
            sep="=",
            yellow=True,
        )
        for test_id in sorted(discarded):
            terminalreporter.write_line(f"  {test_id}")
            for line in str(discarded[test_id]).splitlines():
                terminalreporter.write_line(f"      {line}")
        terminalreporter.write_line("")
        terminalreporter.write_line(
            f"{len(discarded)} test(s) had snapshot drift that was downgraded to a warning "
            "because the test had already reported by the time the discrepancy was detected "
            "(typically: an in-test mismatch was swallowed by user code, and the next test's "
            "first execute_query surfaced it). The plugin could not run these against the real "
            "DB. Re-record with SODA_TEST_SNAPSHOT=record to refresh the snapshots."
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
