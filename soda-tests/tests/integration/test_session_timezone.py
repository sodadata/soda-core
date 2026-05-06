import logging
from datetime import datetime, timezone, tzinfo
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
EXPECTED_SESSION_TIMEZONES: dict[str, tzinfo] = {
    "postgres": timezone.utc,
    "sqlserver": timezone.utc,
    "redshift": timezone.utc,
    "databricks": ZoneInfo("Etc/UTC"),
    "snowflake": ZoneInfo("America/Los_Angeles"),
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
        f"_fetch_session_timezone() returned {result!r} (type {type(result).__name__}); "
        "expected a tzinfo instance"
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
    if expected == actual:
        return

    # Names differ but offsets agree — accept (e.g. "UTC" vs "Etc/UTC" vs +00:00).
    now = datetime.now()
    assert expected.utcoffset(now) == actual.utcoffset(now), (
        f"Expected session TZ {expected!r} for '{test_datasource}' but connection reported {actual!r}"
    )
