from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.utils import execute_scan_and_get_scan_result
from soda.execution.check_outcome import CheckOutcome


def test_vars_in_name(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - row_count > 0:
                name: testing name ${{NAME}}
        """,
        variables={
            "NAME": "something",
        },
    )
    assert scan_result["checks"][0]["name"] == "testing name something"


def test_vars_in_foreach_name(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          for each dataset D:
            datasets:
                - include {table_name}
            checks:
                - row_count > 1:
                    name: Row count in ${{D}} must be positive
        """,
    )
    assert scan_result["checks"][0]["name"].lower() == f"Row count in {table_name} must be positive".lower()


def test_vars_schema_check(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table).lower()

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for ${{table}}:
            - schema:
                fail:
                    when wrong column type:
                        id: text
        """
    )
    scan.add_variables({"table": table_name})

    scan.execute()

    check = scan._checks[0]

    assert check.outcome == CheckOutcome.PASS
