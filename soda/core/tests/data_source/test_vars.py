from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source
from helpers.utils import execute_scan_and_get_scan_result


def test_vars_in_checks(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    date_expr = "" if test_data_source == "sqlserver" else "DATE"

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          filter ${{TABLE_NAME}} [${{PARTITION}}]:
              ${{FILTER_KEY}}: ${{FILTER_COLUMN}} = {date_expr} '${{DATE}}'
          checks for ${{TABLE_NAME}}:
            - ${{CHECK}}:
                name: Row count in ${{TABLE_NAME}} must be ${{NAME_OUTCOME}}

          for each dataset ${{TABLE_ALIAS}}:
            datasets:
                - include ${{TABLE_NAME}}
            checks:
                - ${{CHECK}}:
                    name: Row count in ${{D}} must be ${{NAME_OUTCOME}}
        """,
        variables={
            "TABLE_ALIAS": "D",
            "TABLE_NAME": table_name,
            "PARTITION": "daily",
            "FILTER_KEY": "where",
            "DATE": "2020-06-23",
            "FILTER_COLUMN": "date_updated",
            "FILTER_COLUMN": "cat",
            "FILTER_VALUE": "'HIGH'",
            "CHECK": "row_count > 0",
            "NAME_OUTCOME": "positive",
        },
    )
    for check in scan_result["checks"]:
        assert check["name"].lower() == f"Row count in {table_name} must be positive".lower()
        assert check["table"].lower() == table_name.lower()
