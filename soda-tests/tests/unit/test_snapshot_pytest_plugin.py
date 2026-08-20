"""Integration tests for snapshot_pytest_plugin.

These tests use pytest's ``pytester`` fixture to spawn an inner pytest run with
the plugin loaded, then assert against the captured output. That's the only way
to verify the plugin's hooks fire correctly — unit-testing the hook functions
in isolation would miss the integration (e.g. that the plugin is discoverable,
that the report-status hook actually changes the displayed letter).
"""

from __future__ import annotations

import pytest

pytest_plugins = ["pytester"]


@pytest.fixture
def _reset_rerun_registry():
    """Each rerun-mode pytester run starts with a clean rerun registry."""
    from helpers.snapshot_connection import reset_pending_rerun_record
    from helpers.snapshot_pytest_plugin import _RERAN_TEST_RECORD

    reset_pending_rerun_record()
    _RERAN_TEST_RECORD.clear()
    yield
    reset_pending_rerun_record()
    _RERAN_TEST_RECORD.clear()


def _conftest_text() -> str:
    return 'pytest_plugins = ["helpers.snapshot_pytest_plugin"]\n'


def test_rerun_protocol_intercepts_first_attempt_replay_error(pytester, _reset_rerun_registry):
    """A test that raises SnapshotMismatchError on its first attempt is
    automatically re-run; the second attempt sees the test passing and the
    protocol reports it as RERAN: PASSED."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_rerun="""
import os
from helpers.snapshot_connection import _PENDING_RERUN
from helpers.snapshot_manager import SnapshotMismatchError

_STATE = {"attempt": 0}

def test_target():
    _STATE["attempt"] += 1
    if _STATE["attempt"] == 1:
        _PENDING_RERUN["test_rerun.py::test_target"] = "simulated mismatch"
        raise SnapshotMismatchError("simulated mismatch")
    # Second attempt passes.
    assert _STATE["attempt"] == 2
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(passed=1)
    assert any(
        "RERAN: PASSED" in line for line in result.outlines
    ), "Expected 'RERAN: PASSED' label; got:\n" + "\n".join(result.outlines)
    assert any(
        "snapshot drift" in line for line in result.outlines
    ), "Expected rerun summary section; got:\n" + "\n".join(result.outlines)


def test_rerun_protocol_reports_failure_when_rerun_also_fails(pytester, _reset_rerun_registry):
    """If both first attempt and rerun fail, the test counts as FAILED and the
    label is RERAN: FAILED — we don't try a third time."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_rerun_fail="""
from helpers.snapshot_connection import _PENDING_RERUN
from helpers.snapshot_manager import SnapshotMismatchError

_STATE = {"attempt": 0}

def test_broken():
    _STATE["attempt"] += 1
    if _STATE["attempt"] == 1:
        _PENDING_RERUN["test_rerun_fail.py::test_broken"] = "simulated mismatch"
        raise SnapshotMismatchError("simulated mismatch")
    # Second attempt: a real test failure (not another replay error).
    assert False, "real failure on rerun"
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(failed=1)
    assert any("RERAN: FAILED" in line for line in result.outlines), "\n".join(result.outlines)


def test_rerun_protocol_does_not_fire_in_strict_mode(pytester, _reset_rerun_registry, monkeypatch):
    """With SODA_TEST_SNAPSHOT_STRICT=true a SnapshotMismatchError bubbles up
    as a normal failure — no rerun, no RERAN label. Used for nightly post-
    record verification to catch non-deterministic SQL."""
    monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "true")
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_no_rerun="""
from helpers.snapshot_connection import _PENDING_RERUN
from helpers.snapshot_manager import SnapshotMismatchError

def test_target():
    _PENDING_RERUN["test_no_rerun.py::test_target"] = "ignored"
    raise SnapshotMismatchError("not re-runnable under strict mode")
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(failed=1)
    assert not any("RERAN" in line for line in result.outlines)


def test_rerun_skipped_when_setup_fails(pytester, _reset_rerun_registry):
    """Even if a test queues itself for rerun, a setup-phase failure means
    no rerun is attempted — the test must pass setup/teardown for a rerun
    to make sense (per the design spec)."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_setup_fails="""
import pytest
from helpers.snapshot_connection import _PENDING_RERUN

@pytest.fixture
def broken_fixture():
    raise RuntimeError("setup blew up")

def test_with_broken_setup(broken_fixture):
    _PENDING_RERUN["test_setup_fails.py::test_with_broken_setup"] = "should not be used"
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(errors=1)
    assert not any("RERAN" in line for line in result.outlines)


def test_strict_mode_banner_in_terminal_summary(pytester, _reset_rerun_registry, monkeypatch):
    """In strict mode the terminal summary shows a banner so the user knows
    snapshot mismatches will fail hard rather than rerun."""
    monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "true")
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_plain="""
def test_plain():
    assert True
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(passed=1)
    assert any(
        "SODA_TEST_SNAPSHOT_STRICT=true" in line for line in result.outlines
    ), "Expected strict-mode banner in output; got:\n" + "\n".join(result.outlines)


# ---------------------------------------------------------------------------
# Transient DB errors (SQL Server deadlock victim) → one re-run
# ---------------------------------------------------------------------------

_DEADLOCK_MESSAGE = (
    "('40001', '[40001] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
    "Transaction (Process ID 52) was deadlocked on lock resources with another process "
    "and has been chosen as the deadlock victim. Rerun the transaction. (1205) (SQLFetch)')"
)


def test_transient_deadlock_in_test_body_is_rerun_once(pytester, _reset_rerun_registry):
    """A deadlock-victim error on the first attempt is re-run; the passing rerun
    counts as passed and is listed under the transient section, not snapshot drift."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_deadlock=f"""
_STATE = {{"attempt": 0}}

def test_target():
    _STATE["attempt"] += 1
    if _STATE["attempt"] == 1:
        raise Exception({_DEADLOCK_MESSAGE!r})
    assert _STATE["attempt"] == 2
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(passed=1)
    assert any("RERAN: PASSED" in line for line in result.outlines), "\n".join(result.outlines)
    assert any("transient DB errors" in line for line in result.outlines), "\n".join(result.outlines)
    assert not any("snapshot drift" in line for line in result.outlines), "\n".join(result.outlines)


def test_transient_deadlock_in_fixture_teardown_is_rerun_once(pytester, _reset_rerun_registry):
    """Catalog deadlocks mostly strike fixture DDL (setup/teardown). A teardown
    deadlock on the first attempt must also trigger the rerun."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_teardown_deadlock=f"""
import pytest

_STATE = {{"attempt": 0}}

@pytest.fixture
def table():
    _STATE["attempt"] += 1
    yield "t"
    if _STATE["attempt"] == 1:
        raise Exception({_DEADLOCK_MESSAGE!r})

def test_target(table):
    assert table == "t"
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(passed=1, errors=0)
    assert any("RERAN: PASSED" in line for line in result.outlines), "\n".join(result.outlines)


def test_transient_deadlock_on_both_attempts_fails(pytester, _reset_rerun_registry):
    """Only one rerun: a second deadlock surfaces as a failure."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_deadlock_twice=f"""
def test_target():
    raise Exception({_DEADLOCK_MESSAGE!r})
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(failed=1)
    assert any("RERAN: FAILED" in line for line in result.outlines), "\n".join(result.outlines)


def test_unrelated_failure_is_not_rerun(pytester, _reset_rerun_registry):
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_plain_failure="""
_STATE = {"attempt": 0}

def test_target():
    _STATE["attempt"] += 1
    assert _STATE["attempt"] == 2, "a real assertion failure"
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(failed=1)
    assert not any("RERAN" in line for line in result.outlines), "\n".join(result.outlines)


def test_transient_deadlock_is_rerun_even_in_strict_mode(pytester, _reset_rerun_registry, monkeypatch):
    """Strict mode only disables snapshot-drift reruns; a transient DB error is
    unrelated to snapshot fidelity and still gets its one rerun."""
    monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "true")
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_strict_deadlock=f"""
_STATE = {{"attempt": 0}}

def test_target():
    _STATE["attempt"] += 1
    if _STATE["attempt"] == 1:
        raise Exception({_DEADLOCK_MESSAGE!r})
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(passed=1)
    assert any("RERAN: PASSED" in line for line in result.outlines), "\n".join(result.outlines)


def test_transient_deadlock_in_setup_on_both_attempts_is_reported(pytester, _reset_rerun_registry):
    """A rerun that fails again in *setup* has no call report; it must still count
    as an error, be labelled, and be listed in the transient summary."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_setup_deadlock_twice=f"""
import pytest

@pytest.fixture
def table():
    raise Exception({_DEADLOCK_MESSAGE!r})

def test_target(table):
    pass
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(errors=1)
    assert any("RERAN: ERROR" in line for line in result.outlines), "\n".join(result.outlines)
    assert any("transient DB errors" in line for line in result.outlines), "\n".join(result.outlines)
    assert any("test_setup_deadlock_twice.py::test_target" in line for line in result.outlines)


def test_transient_deadlock_in_teardown_on_both_attempts_is_reported(pytester, _reset_rerun_registry):
    """A rerun whose *teardown* deadlocks again: the call passed, but the test must
    surface as an error and be labelled as a rerun error, not RERAN: PASSED only."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_teardown_deadlock_twice=f"""
import pytest

@pytest.fixture
def table():
    yield "t"
    raise Exception({_DEADLOCK_MESSAGE!r})

def test_target(table):
    assert table == "t"
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(passed=1, errors=1)
    assert any("RERAN: ERROR" in line for line in result.outlines), "\n".join(result.outlines)
    assert any("transient DB errors" in line for line in result.outlines), "\n".join(result.outlines)
