import logging
import os
import sys
from logging import Handler, LogRecord
from textwrap import indent
from typing import Tuple

from soda.common.exception_helper import get_exception_stacktrace
from soda.common.file_system import file_system
from soda.common.table_printer import TablePrinter
from soda.execution.data_type import DataType
from tests.helpers.common_test_tables import customers_test_table, orders_test_table
from tests.helpers.data_source_fixture import DataSourceFixture
from tests.helpers.test_table import TestTable


class Script:
    lines = []


def append_to_script(line=""):
    Script.lines.append(line)


class WrappedCursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self.description = None

    def execute(self, sql: str):
        execute_result = self.cursor.execute(sql)
        self.description = self.cursor.description
        return execute_result

    def fetchall(self) -> Tuple[tuple]:
        rows = self.cursor.fetchall()
        column_names = TablePrinter.get_column_names_from_cursor_description(self.cursor)
        table_as_comment = TablePrinter(rows=rows, column_names=column_names).get_table(prefix="    # ")
        append_to_script("    # Previous SQL query should produce:")
        append_to_script(table_as_comment)
        append_to_script()
        return rows

    def fetchone(self) -> tuple:
        row = self.cursor.fetchone()
        column_names = TablePrinter.get_column_names_from_cursor_description(self.cursor)
        table_as_comment = TablePrinter(rows=[row], column_names=column_names).get_table(prefix="    # ")
        append_to_script("    # Previous SQL query should produce:")
        append_to_script(table_as_comment)
        append_to_script()
        return row

    def close(self):
        self.cursor.close()


class WrappedConnection:
    def __init__(self, connection):
        self.connection = connection

    def cursor(self) -> WrappedCursor:
        return WrappedCursor(self.connection.cursor())

    def close(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()


def wrap_connection(data_source_fixture):
    data_source_fixture.data_source.connection = WrappedConnection(data_source_fixture.data_source.connection)


def generate_compatibility_script():
    integration_dir = __file__[: -len("/generate_compatibility_script.py")]
    script_file_path = f"{integration_dir}/compatibility_script.py"

    compatibility_script = file_system().file_read_as_str(script_file_path)
    first_execute_index = compatibility_script.find("\nexecute(") + 1
    first_part = compatibility_script[: first_execute_index - 1]
    append_to_script(first_part)

    os.environ["POSTGRES_REUSE_SCHEMA"] = "DISABLED"
    configure_sql_logging()

    data_source_fixture = DataSourceFixture._create()
    data_source_fixture._test_session_starts()
    wrap_connection(data_source_fixture)

    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)
    case_sensitive_table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="CaseSensitive",
            columns=[("Id", DataType.TEXT)],
            quote_names=True,
        )
    )

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          discover tables:
            datasets:
              - include {table_name}
          automated monitoring:
            datasets:
              - include {table_name}
          profile columns:
            columns:
              - {table_name}.size
              - {table_name}.sizeTxt
          checks for {table_name}:
            - row_count = 10.0
            - row_count = 3:
                filter: cat = 'HIGH'
            - missing_percent(pct) < 35:
            - invalid_percent(pct) < 35 %:
                  valid format: percentage
            - anomaly score for row_count < default
            - min_length(cat) = 3
            - max_length(cat) = 6
            - avg_length(cat) = 4.2
            - stddev(size) between 3.26 and 3.27
            - stddev_pop(size) between 3.02 and 3.03
            - stddev_samp(size) between 3.26 and 3.27
            - variance(size) between 10.65 and 10.66
            - var_pop(size) between 9.13 and 9.14
            - var_samp(size) between 10.65 and 10.66
            - percentile(distance, 0.7) = 999
            - freshness(ts) < 1d
            - schema:
                fail:
                  when schema changes: any

          checks for {orders_table_name}:
            - values in customer_id_nok must exist in {table_name} id

          checks for \"{case_sensitive_table_name}\":
            - row_count >= 0
        """
    )
    scan.execute_unchecked()
    # scan.assert_no_error_logs()

    data_source_fixture._test_session_ends()

    script_txt = "\n".join(Script.lines)
    file_system().file_write_from_str(script_file_path, script_txt)
    print(script_txt)


class SqlHandler(Handler):
    def __init__(self):
        super().__init__()

    def emit(self, record: LogRecord) -> None:
        msg = record.msg
        if record.name == "tests.helpers.data_source_fixture":
            prefix = "  # Test data handler update: \n  #   "
            if msg.startswith(prefix):
                ddl = msg[len(prefix) :]
                ddl = ddl.replace("\n  #", "\n")
                self.print_sql(f"{ddl}\n")
        elif msg.startswith("Query "):
            if not msg.startswith("Query error:"):
                colon_index = msg.find(":\n")
                query_name = msg[6:colon_index]
                if query_name.startswith("postgres."):
                    query_name = query_name[9:]
                sql = msg[colon_index + 2 :]
                self.print_sql(sql, query_name)
        else:
            pass
            # msg = msg.replace("\n", "")
            # append_sql(f"          [NoSQL] {msg}")

    def print_sql(self, sql: str, header: str = None):
        append_to_script()
        if header:
            append_to_script(f"# {header}")

        append_to_script(f"execute(")
        append_to_script(f'    """')
        sql = sql.replace('"', '\\"')
        append_to_script(f'{indent(sql.strip(), "        ")}')
        append_to_script(f'    """')
        append_to_script(f")")


def configure_sql_logging():
    sys.stderr = sys.stdout
    logging.getLogger("tests.helpers.data_source_fixture").setLevel(logging.DEBUG)
    logging.getLogger("soda.scan").setLevel(logging.DEBUG)

    logging.basicConfig(
        level=logging.ERROR,
        force=True,  # Override any previously set handlers.
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        # %(name)s
        format="%(message)s",
        handlers=[SqlHandler()],
    )


if __name__ == "__main__":
    try:
        generate_compatibility_script()
    except Exception as e:
        print(get_exception_stacktrace(e))
