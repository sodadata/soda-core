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
def _reset_fallback_registry():
    """Each inner pytest run starts with a clean registry. Outer test relies on
    the plugin's pytest_configure to clear it, but we belt-and-braces here so
    interleaved tests don't pollute each other."""
    from helpers.snapshot_connection import reset_fallback_test_record

    reset_fallback_test_record()
    yield
    reset_fallback_test_record()


def _conftest_text() -> str:
    return 'pytest_plugins = ["helpers.snapshot_pytest_plugin"]\n'


def _inner_test_recording_fallback(test_id: str, reason: str = "mismatch on op #3") -> str:
    """Return source for a test that registers a fallback entry then passes.

    The plugin's reporting path only cares about the registry — it doesn't need
    a real SnapshotDataSourceConnection. By driving the registry directly we
    keep the test fast and free of DB setup.
    """
    # Note: the inner test runs in its own pytest invocation. PYTEST_CURRENT_TEST
    # is set automatically by pytest there.
    return f"""
from helpers.snapshot_connection import _FALLBACK_TEST_RECORD

def test_target():
    # Simulate snapshot fallback being triggered for THIS test's nodeid.
    _FALLBACK_TEST_RECORD["{test_id}"] = "{reason}"
"""


def test_passing_test_with_fallback_is_displayed_as_fallback(pytester, _reset_fallback_registry):
    """A passing test that triggered fallback should show ``FALLBACK`` (not ``PASSED``)
    in verbose output, and contribute to a fallback summary section."""
    pytester.makeconftest(_conftest_text())
    test_id = "test_passing_fallback.py::test_target"
    pytester.makepyfile(test_passing_fallback=_inner_test_recording_fallback(test_id))

    result = pytester.runpytest("-v")

    # Real outcome is preserved: the test counts as passed.
    result.assert_outcomes(passed=1)
    # Verbose output shows "FALLBACK: PASSED" in place of plain "PASSED".
    assert any(
        "FALLBACK: PASSED" in line for line in result.outlines
    ), "Expected 'FALLBACK: PASSED' verbose label; got:\n" + "\n".join(result.outlines)
    # End-of-session section lists the test and its reason.
    assert any(
        "snapshot fallback triggered" in line for line in result.outlines
    ), "Expected fallback summary section; got:\n" + "\n".join(result.outlines)
    assert any(test_id in line for line in result.outlines)
    assert any("mismatch on op #3" in line for line in result.outlines)


def test_failing_test_with_fallback_is_displayed_as_fallback_failed(pytester, _reset_fallback_registry):
    """A failing test that also triggered fallback should still count as failed
    (exit code, summary), but the verbose label and short-char colour flag the
    fallback so the reader sees both signals."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_failing_fallback="""
from helpers.snapshot_connection import _FALLBACK_TEST_RECORD

def test_target():
    _FALLBACK_TEST_RECORD["test_failing_fallback.py::test_target"] = "mismatch reason"
    assert False, "intentional"
"""
    )

    result = pytester.runpytest("-v")
    # Real outcome is preserved: counts as failed.
    result.assert_outcomes(failed=1)
    assert result.ret != 0
    assert any(
        "FALLBACK: FAILED" in line for line in result.outlines
    ), "Expected 'FALLBACK: FAILED' verbose label; got:\n" + "\n".join(result.outlines)
    # Summary section still lists this test.
    assert any("snapshot fallback triggered" in line for line in result.outlines)


def test_no_summary_section_when_no_fallback(pytester, _reset_fallback_registry):
    """Sessions with no fallback events should not print the summary section."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_plain="""
def test_plain():
    assert True
"""
    )

    result = pytester.runpytest("-v")
    result.assert_outcomes(passed=1)
    assert not any("snapshot fallback triggered" in line for line in result.outlines)


def test_fallback_test_count_in_summary_message(pytester, _reset_fallback_registry):
    """Summary line should report the correct fallback count."""
    pytester.makeconftest(_conftest_text())
    pytester.makepyfile(
        test_multi="""
from helpers.snapshot_connection import _FALLBACK_TEST_RECORD

def test_a():
    _FALLBACK_TEST_RECORD["test_multi.py::test_a"] = "reason A"

def test_b():
    _FALLBACK_TEST_RECORD["test_multi.py::test_b"] = "reason B"

def test_c():
    pass  # no fallback
"""
    )

    result = pytester.runpytest("-v")
    # All 3 still count as passed (categories preserved).
    result.assert_outcomes(passed=3)
    assert any(
        "2 test(s) triggered snapshot fallback" in line for line in result.outlines
    ), "Expected fallback count of 2 in summary; got:\n" + "\n".join(result.outlines)
