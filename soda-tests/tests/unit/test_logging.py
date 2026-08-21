import logging
import os
import warnings

from soda_core.common.logging_configuration import (
    SodaConsoleFormatter,
    configure_library_warning_capture,
    configure_logging,
)
from soda_core.common.logging_constants import Emoticons
from soda_core.common.logs import Logs

MAX_CHARS_PER_STRING = int(os.environ.get("SODA_DEBUG_PRINT_VALUE_MAX_CHARS", 256))
MAX_ROWS = int(os.environ.get("SODA_DEBUG_PRINT_RESULT_MAX_ROWS", 20))
MAX_CHARS_PER_SQL = int(os.environ.get("SODA_DEBUG_PRINT_SQL_MAX_CHARS", 1024))


def test_logging_debug_prints(data_source_test_helper, monkeypatch):
    """Test that query results are correctly truncated.

    The rendered table is the payload firehose (R-657 task 1): opt in via
    SODA_LOG_PAYLOADS to get the truncation behaviour under test here.
    """
    monkeypatch.setenv("SODA_LOG_PAYLOADS", "true")

    multi_row_sql = """
        with t0 as (
        select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all
        select 6 union all select 7 union all select 8 union all select 9 union all select 10 union all
        select 11 union all select 12 union all select 13 union all select 14 union all select 15 union all
        select 16 union all select 17 union all select 18 union all select 19 union all select 20 union all
        select 21 union all select 22 union all select 23 union all select 24 union all select 25 union all
        select 26 union all select 27 union all select 28 union all select 29 union all select 30 union all
        select 31 union all select 32 union all select 33 union all select 34 union all select 35 union all
        select 36 union all select 37 union all select 38 union all select 39 union all select 40 union all
        select 41 union all select 42 union all select 43 union all select 44 union all select 45 union all
        select 46 union all select 47 union all select 48 union all select 49 union all select 50 union all
        select 51 union all select 52 union all select 53 union all select 54 union all select 55 union all
        select 56 union all select 57 union all select 58 union all select 59 union all select 60 union all
        select 61 union all select 62 union all select 63 union all select 64 union all select 65 union all
        select 66 union all select 67 union all select 68 union all select 69 union all select 70 union all
        select 71 union all select 72 union all select 73 union all select 74 union all select 75 union all
        select 76 union all select 77 union all select 78 union all select 79 union all select 80 union all
        select 81 union all select 82 union all select 83 union all select 84 union all select 85 union all
        select 86 union all select 87 union all select 88 union all select 89 union all select 90 union all
        select 91 union all select 92 union all select 93 union all select 94 union all select 95 union all
        select 96 union all select 97 union all select 98 union all select 99 union all select 100
        )
        select * from t0 order by 1
        """  #  some databases don't preserve order UNION ALLs (Trino), hence order by

    logs = Logs()
    data_source_test_helper.data_source_impl.execute_query(multi_row_sql)
    log_lines = logs.get_logs()
    sql_print_log = [l for l in log_lines if l.startswith("SQL query fetchall in datasource")][0]
    if len(multi_row_sql) > MAX_CHARS_PER_SQL:
        truncated_sql = multi_row_sql[: MAX_CHARS_PER_SQL - 3] + "..."
        assert truncated_sql in sql_print_log

    query_result_log = [l for l in log_lines if l.startswith("SQL query result rows")][0]
    assert str(MAX_ROWS) in query_result_log
    if MAX_ROWS < 99:
        assert str(MAX_ROWS + 1) not in query_result_log

    logs = Logs()
    long_string = "a" * 10000
    sql_long_string = f"select '{long_string}', 'bbb', 'ccc'"
    data_source_test_helper.data_source_impl.execute_query(sql_long_string)
    log_lines = logs.get_logs()

    query_result_log = [l for l in log_lines if l.startswith("SQL query result rows")][0]
    if len(long_string) > MAX_CHARS_PER_STRING:
        assert long_string[: MAX_CHARS_PER_STRING - 3] + "..." in query_result_log
    else:
        assert long_string in query_result_log
    assert "bbb" in query_result_log
    assert "ccc" in query_result_log


def _make_record(level: int, message: str) -> logging.LogRecord:
    return logging.LogRecord(
        name="soda",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


class TestSodaConsoleFormatterTimestampAndLevel:
    """R-657 task 3: console-only gap — Cloud already receives timestamp/level as
    structured fields, but the console formatter used to comment these out."""

    def test_format_leads_with_timestamp_then_level_then_message(self):
        formatter = SodaConsoleFormatter()
        record = _make_record(logging.INFO, "hello")

        parts = formatter.format(record).split(" | ")

        assert parts[0] == formatter.format_timestamp(record)
        assert parts[1] == "INF"
        assert parts[2] == "hello"

    def test_error_level_keeps_police_car_prefix_on_the_message_part(self):
        formatter = SodaConsoleFormatter()
        record = _make_record(logging.ERROR, "boom")

        parts = formatter.format(record).split(" | ")

        assert parts[1] == "ERR"
        assert parts[2] == f"{Emoticons.POLICE_CAR_LIGHT} boom"

    def test_debug_level_renders_as_deb(self):
        formatter = SodaConsoleFormatter()
        record = _make_record(logging.DEBUG, "trace")

        parts = formatter.format(record).split(" | ")

        assert parts[1] == "DEB"


def test_configure_logging_invokes_warning_capture_policy(monkeypatch):
    """IMPORTANT: nothing else pins that configure_logging actually wires up the
    warning-capture policy — deleting the call would leave the rest of the suite
    green (configure_library_warning_capture is only exercised directly elsewhere).
    Spy on the call itself."""
    calls = []
    monkeypatch.setattr(
        "soda_core.common.logging_configuration.configure_library_warning_capture",
        lambda verbose: calls.append(verbose),
    )

    try:
        configure_logging(verbose=True)
        assert calls == [True]

        calls.clear()
        configure_logging(verbose=False)
        assert calls == [False]
    finally:
        # configure_logging mutates global logging state (root level/handlers via
        # basicConfig(force=True)); undo the spy first so this restores the real
        # policy, matching what conftest's session-start call configured.
        monkeypatch.undo()
        configure_logging(verbose=True)


class TestLibraryWarningCapturePolicy:
    """R-657 task 4: library warnings (pandas/numpy/sklearn FutureWarning/
    RuntimeWarning, heavily used by the soda-rad extension) are routed through
    ``py.warnings`` instead of leaking straight to stderr, and are silenced under
    default (non-verbose) runs so they don't add user-visible noise."""

    def setup_method(self):
        # The session (conftest) runs configure_logging(verbose=True) once up
        # front, which leaves py.warnings at DEBUG. Save that so teardown restores
        # it instead of leaking whichever level the last test in this class set.
        self._original_py_warnings_level = logging.getLogger("py.warnings").level

    def teardown_method(self):
        logging.captureWarnings(False)
        logging.getLogger("py.warnings").setLevel(self._original_py_warnings_level)

    def test_library_warning_reaches_the_stream_when_verbose(self, caplog):
        configure_library_warning_capture(verbose=True)
        # Deliberately not scoped to "py.warnings": that would override the level
        # configure_library_warning_capture just set, defeating the test. This only
        # lowers the root logger + caplog's own handler threshold so a record that
        # does get past "py.warnings" is captured.
        caplog.set_level(logging.DEBUG)

        with warnings.catch_warnings():
            warnings.simplefilter("always")
            warnings.warn("numeric library noise", FutureWarning)

        py_warning_records = [r for r in caplog.records if r.name == "py.warnings"]
        assert len(py_warning_records) == 1
        assert "numeric library noise" in py_warning_records[0].getMessage()

    def test_library_warning_does_not_reach_the_stream_when_not_verbose(self, caplog):
        configure_library_warning_capture(verbose=False)
        caplog.set_level(logging.DEBUG)

        with warnings.catch_warnings():
            warnings.simplefilter("always")
            warnings.warn("numeric library noise", FutureWarning)

        assert [r for r in caplog.records if r.name == "py.warnings"] == []
