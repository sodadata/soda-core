import logging
import os
import sys
from logging import Handler, LogRecord

from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.data_source_fixture import DataSourceFixture


def generate_compatibility_script():
    os.environ["POSTGRES_REUSE_SCHEMA"] = "DISABLED"
    configure_sql_logging()

    data_source_fixture = DataSourceFixture._create()
    data_source_fixture._test_session_starts()

    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          discover tables:
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
            - missing_percent(pct) < 35:
            - invalid_percent(pct) < 35 %:
                  valid format: percentage
            - stddev(size) between 3.26 and 3.27
            - stddev_pop(size) between 3.02 and 3.03
            - stddev_samp(size) between 3.26 and 3.27
            - variance(size) between 10.65 and 10.66
            - var_pop(size) between 9.13 and 9.14
            - var_samp(size) between 10.65 and 10.66
            - percentile(distance, 0.7) = 999
            - schema:
                fail:
                  - when changes: any
        """
    )
    scan.execute_unchecked()

    data_source_fixture._test_session_ends()


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
                print(f"{ddl}\n")
        elif msg.startswith("Query "):
            if not msg.startswith("Query error:"):
                colon_index = msg.find(":\n")
                query_name = msg[6:colon_index]
                if query_name.startswith("postgres."):
                    query_name = query_name[9:]
                sql = msg[colon_index + 2 :]
                print(f"# {query_name}")
                print(f"{sql}\n")

        else:
            pass
            # msg = msg.replace("\n", "")
            # print(f"          [NoSQL] {msg}")


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
    generate_compatibility_script()
