"""Pytest plugin that surfaces snapshot fallback events in test output.

When ``SODA_TEST_SNAPSHOT=replay`` + ``SODA_TEST_SNAPSHOT_FALLBACK=true`` is
active, a test whose recorded SQL no longer matches the production SQL falls
back to the real DB transparently. The test still passes (assuming the new SQL
is correct), so it would silently mask SQL drift in the snapshot recordings.

This plugin makes fallback visible while preserving the test's real outcome:

* progress char: standard ``.`` / ``F`` (passed / failed) but coloured yellow
  for any test that fell back, so fallback rows stand out without losing the
  pass-vs-fail signal
* verbose status: ``FALLBACK: PASSED`` or ``FALLBACK: FAILED`` instead of the
  plain ``PASSED`` / ``FAILED`` — the original outcome is preserved so the
  exit code and pytest summary counts stay correct
* end-of-session "snapshot fallback summary" section listing every test that
  fell back, with the first triggering reason
* an exit hint reminding the user to re-record snapshots

Registered via ``pytest_plugins = ["helpers.snapshot_pytest_plugin"]`` in each
repo's ``conftest.py`` so it works for soda-core, soda-extensions, and any
downstream repo that consumes soda-tests.
"""

from __future__ import annotations

from typing import Optional


def pytest_configure(config) -> None:  # noqa: D401
    """Reset the per-process fallback registry at session start.

    Stale entries can leak across pytest invocations when running under
    ``pytest --pdb-trace`` style workflows that reuse the interpreter. Clearing
    on configure makes each session start with a clean slate.
    """
    from helpers.snapshot_connection import reset_fallback_test_record

    reset_fallback_test_record()


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
    """Annotate the call-phase status for tests that fell back, preserving
    pass/fail. The pytest summary line still shows the test under ``passed=``
    or ``failed=`` — only the verbose label changes.

    The short progress char stays a plain ASCII string. Pytest concatenates
    those into a buffer and later calls ``.rsplit`` on the result, so a
    markup tuple here triggers ``AttributeError: 'tuple' object has no
    attribute 'rsplit'`` deep inside pytest's terminal printer. The verbose
    label is safe to colourize because it's only handed to the markup-aware
    write path.
    """
    if report.when != "call":
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
    """Print a clear section listing every test that triggered fallback."""
    from helpers.snapshot_connection import get_fallback_test_record

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


def _yellow(text: str) -> tuple:
    """Return the (text, markup) tuple pytest expects for coloured short status.

    Pytest's ``pytest_report_teststatus`` accepts either a plain string or a
    ``(text, {"yellow": True})`` style tuple for the short letter / verbose
    label. Wrapping in a tuple gives us colour without pulling in colorama.
    """
    return (text, {"yellow": True, "bold": True})
