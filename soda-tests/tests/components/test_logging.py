import os

from soda_core.common.logs import Logs

MAX_CHARS_PER_STRING = int(os.environ.get("SODA_DEBUG_PRINT_VALUE_MAX_CHARS", 256))
MAX_ROWS = int(os.environ.get("SODA_DEBUG_PRINT_RESULT_MAX_ROWS", 20))
MAX_CHARS_PER_SQL = int(os.environ.get("SODA_DEBUG_PRINT_SQL_MAX_CHARS", 1024))


def test_logging_debug_prints(data_source_test_helper):
    """Test that query results are correctly truncated."""

    multi_row_sql = """
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
        select 96 union all select 97 union all select 98 union all select 99 union all select 100"""

    logs = Logs()
    data_source_test_helper.data_source_impl.execute_query(multi_row_sql)
    log_lines = logs.get_logs()
    sql_print_log = [l for l in log_lines if l.startswith("SQL query fetchall in datasource")][0]
    if len(multi_row_sql) > MAX_CHARS_PER_SQL:
        truncated_sql = multi_row_sql[: MAX_CHARS_PER_SQL - 3] + "..."
        assert truncated_sql in sql_print_log

    query_result_log = [l for l in log_lines if l.startswith("SQL query result")][0]
    assert str(MAX_ROWS) in query_result_log
    if MAX_ROWS < 99:
        assert str(MAX_ROWS + 1) not in query_result_log

    logs = Logs()
    long_string = "a" * 10000
    sql_long_string = f"select '{long_string}', 'bbb', 'ccc'"
    data_source_test_helper.data_source_impl.execute_query(sql_long_string)
    log_lines = logs.get_logs()

    query_result_log = [l for l in log_lines if l.startswith("SQL query result")][0]
    if len(long_string) > MAX_CHARS_PER_STRING:
        assert long_string[: MAX_CHARS_PER_STRING - 3] + "..." in query_result_log
    else:
        assert long_string in query_result_log
    assert "bbb" in query_result_log
    assert "ccc" in query_result_log
