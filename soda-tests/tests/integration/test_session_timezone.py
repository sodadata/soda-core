import logging
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Union
from zoneinfo import ZoneInfo

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_fixtures import test_datasource
from soda_core.common.logs import Logs

# What session timezone we expect each test datasource to report.
#
# This mapping is the inverse of what the engine code does: the engine asks the live connection
# for its session TZ; here we encode what the *test instance* is configured to use, so a
# regression that silently flips a test datasource's session TZ (and would change the
# diagnostics-warehouse value mappers' behavior) is caught loudly here instead of debugging it
# later as a row-comparison drift.
#
# Some adapters (DuckDB, Trino without an explicit session property) inherit the host's local
# timezone — for those we expect whatever the runner's OS reports, not a fixed string. They are
# in EXPECTED_HOST_LOCAL_DATASOURCES below.
#
# The value can be either a single ``tzinfo`` or a tuple of acceptable ``tzinfo`` values. A tuple
# is used when more than one TZ is legitimately observable across the test environments — for
# example, snapshot-replay returning a value baked in at recording time vs the live database
# returning a different (also-correct) value because the account was reconfigured since. The
# match is "actual equals or is offset-equivalent to *any* of the expected entries". A tuple
# does NOT relax the test: each entry is still strictly checked, the test fails only when the
# actual matches none of them.
_TzExpected = Union[tzinfo, tuple[tzinfo, ...]]

EXPECTED_SESSION_TIMEZONES: dict[str, _TzExpected] = {
    "postgres": timezone.utc,
    "sqlserver": timezone.utc,
    "redshift": timezone.utc,
    "databricks": ZoneInfo("Etc/UTC"),
    # Snapshot replay returns ``timezone.utc`` (recorded when the CI Snowflake account
    # was on UTC); live runs against the current CI account return
    # ``America/Los_Angeles``. Both are correct values that can appear depending on
    # whether snapshots are active for this run, so accept either. A future flip of
    # the CI account would still need a deliberate update here — this is not a free
    # pass for arbitrary drift.
    "snowflake": (ZoneInfo("America/Los_Angeles"), timezone.utc),
    "bigquery": timezone.utc,  # BigQuery is UTC-only by design
    "athena": timezone.utc,  # Athena is UTC-only
    "fabric": timezone.utc,  # Inherits SQL Server adapter; test instance is UTC
    "synapse": timezone.utc,  # Inherits SQL Server adapter; test instance is UTC
    "sparkdf": timezone.utc,  # Connection setup forces SET TIME ZONE 'UTC'
    "oracle": timezone.utc,  # Test instance is UTC
    "dremio": timezone.utc,  # Dremio is UTC-only
    "db2": timezone.utc,  # Test instance is UTC; DB2 modules not always installed locally
    "db2z": timezone.utc,
}

# For these adapters the session TZ follows the host running the test (no client-side override).
EXPECTED_HOST_LOCAL_DATASOURCES: set[str] = {"duckdb", "trino"}


def test_get_session_timezone_returns_tzinfo(data_source_test_helper: DataSourceTestHelper):
    """The connection exposes a tzinfo describing its current session timezone.

    Engine code (the diagnostics warehouse value mappers) relies on this to canonicalize
    timestamp values during cross-source transfer. If a vendor adapter cannot report its
    timezone, the base class falls back to UTC — either way the returned object must be
    usable to localize a naive datetime without raising.
    """
    connection = data_source_test_helper.data_source_impl.data_source_connection

    session_tz = connection.get_session_timezone()

    assert isinstance(session_tz, tzinfo)
    naive = datetime(2024, 1, 1, 12, 0, 0)
    aware = naive.replace(tzinfo=session_tz)
    assert aware.tzinfo is session_tz
    aware.astimezone(timezone.utc)


def test_get_session_timezone_is_cached(data_source_test_helper: DataSourceTestHelper):
    """Repeated calls return the same instance — the connection caches after the first query
    so the engine doesn't issue a SHOW TIMEZONE round-trip per row transferred."""
    connection = data_source_test_helper.data_source_impl.data_source_connection

    first = connection.get_session_timezone()
    second = connection.get_session_timezone()

    assert first is second


def test_fetch_session_timezone_emits_no_errors(data_source_test_helper: DataSourceTestHelper):
    """Calling _fetch_session_timezone() against a real connection must not produce error or
    warning log records.

    The base class swallows any exception and falls back to UTC plus a ``logger.warning``,
    which would mask a broken adapter implementation (wrong SQL, wrong column index, type cast
    failure). By capturing logs around an explicit fetch call and asserting nothing at WARNING
    or above, this test will fail loudly if a vendor's session-TZ query stops working — for
    example after a driver upgrade changes the cursor description shape.
    """
    connection = data_source_test_helper.data_source_impl.data_source_connection
    # The result is cached on first call; bypass the cache so we actually exercise the SQL
    # round-trip on this attempt.
    connection._session_timezone_cache = None

    captured_logs = Logs()
    try:
        result = connection._fetch_session_timezone()
    finally:
        captured_logs.remove_from_root_logger()

    assert isinstance(result, tzinfo), (
        f"_fetch_session_timezone() returned {result!r} (type {type(result).__name__}); " "expected a tzinfo instance"
    )

    error_records = [r for r in captured_logs.get_log_records() if r.levelno >= logging.WARNING]
    if error_records:
        rendered = "\n".join(f"  [{r.levelname}] {r.getMessage()}" for r in error_records)
        pytest.fail(
            f"_fetch_session_timezone() emitted {len(error_records)} warning/error log "
            f"record(s) for datasource '{test_datasource}':\n{rendered}"
        )


def test_get_session_timezone_matches_expected(data_source_test_helper: DataSourceTestHelper):
    """The reported session TZ matches what we expect the test instance to be configured with.

    Catches accidental drift: if Snowflake CI flips its account default TZ, or someone removes
    the explicit ``SET TIMEZONE`` from a test bootstrap, this fails loudly rather than later
    showing up as a wallclock-mismatch in cross-source DWH tests.
    """
    if test_datasource not in EXPECTED_SESSION_TIMEZONES and test_datasource not in EXPECTED_HOST_LOCAL_DATASOURCES:
        pytest.skip(f"No expected session TZ recorded for datasource '{test_datasource}'")

    actual = data_source_test_helper.data_source_impl.data_source_connection.get_session_timezone()

    if test_datasource in EXPECTED_HOST_LOCAL_DATASOURCES:
        host_local = datetime.now().astimezone().tzinfo
        # Compare by *current offset* rather than identity — host_local is a datetime.timezone
        # offset object, while the connection may return a ZoneInfo with the same effective
        # offset right now. Identity / name comparison would be too brittle.
        now = datetime.now()
        assert host_local.utcoffset(now) == actual.utcoffset(now), (
            f"{test_datasource} session TZ ({actual!r}) does not match host local TZ "
            f"({host_local!r}) at the current instant"
        )
        return

    expected = EXPECTED_SESSION_TIMEZONES[test_datasource]
    expected_options: tuple[tzinfo, ...] = expected if isinstance(expected, tuple) else (expected,)

    if _matches_any_expected(actual, expected_options):
        return

    pytest.fail(
        f"Expected session TZ for '{test_datasource}' to match any of {expected_options!r} "
        f"but connection reported {actual!r}. Two distinct named zones can share an offset "
        "at one instant and differ on DST or historical entries — strict identity is required "
        "for a non-UTC expected TZ. UTC-equivalent expectations accept any zero-offset actual."
    )


def _matches_any_expected(actual: tzinfo, expected_options: tuple[tzinfo, ...]) -> bool:
    """Return True if ``actual`` matches any of the ``expected_options``.

    Per-option matching:
    * Strict ``==`` first. ``ZoneInfo`` keys compared by identity, ``timezone(...)``
      offsets compared by offset value.
    * For UTC-equivalent expectations (``utcoffset(now) == 0``), additionally accept
      any actual TZ that reports zero offset right now. This covers the
      ``UTC`` / ``Etc/UTC`` / ``+00:00`` / ``Universal`` family without requiring
      every alias to be enumerated.
    * For non-UTC expectations, require strict ``==`` only — two distinct ZoneInfo
      keys (e.g. ``America/Los_Angeles`` vs ``America/Vancouver``) can share an
      offset at one instant but differ on DST or historical entries; offset-only
      comparison would silently accept the wrong zone.
    """
    now = datetime.now()
    actual_offset = actual.utcoffset(now)
    for expected in expected_options:
        if expected == actual:
            return True
        if expected.utcoffset(now) == timedelta(0) and actual_offset == timedelta(0):
            return True
    return False
