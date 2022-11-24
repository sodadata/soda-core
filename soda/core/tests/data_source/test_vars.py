import pytest
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source
from helpers.utils import execute_scan_and_get_scan_result


@pytest.mark.parametrize(
    "checks",
    [
        pytest.param(
            """
            filter ${TABLE_NAME} [${PARTITION}]:
                ${FILTER_KEY}: ${FILTER_COLUMN_DATE} = {date_expr} '${some_time}'
            checks for ${TABLE_NAME}:
                - ${CHECK}:
                    name: Row count in ${TABLE_NAME} must be ${NAME_OUTCOME}
            variables:
                some_time: '${DATE}'""",
            id="partition filter",
        ),
        pytest.param(
            """
            checks for ${TABLE_NAME}:
                - row_count > 0:
                    filter: ${FILTER_COLUMN} = ${FILTER_VALUE}
                    name: Row count in ${TABLE_NAME} must be ${NAME_OUTCOME}
            variables:
                some_time: '${DATE}'""",
            id="in check filter",
        ),
        pytest.param(
            """
            for each dataset ${TABLE_ALIAS}:
                datasets:
                    - include ${TABLE_NAME}
                checks:
                    - ${CHECK}:
                        name: Row count in ${TABLE_NAME} must be ${NAME_OUTCOME}
            variables:
                some_time: '${DATE}'""",
            id="foreach",
        ),
    ],
)
def test_vars_in_checks(checks: str, data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    date_expr = "" if test_data_source == "sqlserver" else "DATE"
    checks = checks.replace("{date_expr}", date_expr)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        checks,
        variables={
            "TABLE_ALIAS": "D",
            "TABLE_NAME": table_name,
            "PARTITION": "daily",
            "FILTER_KEY": "where",
            # Date passed in as string, but this is replaced by jinja without quotes, yaml would consider it a datetime when loaded,
            # so quotes can be added either here or in the variable definition in the SodaCL variables section.
            "DATE": "2020-06-23",
            "FILTER_COLUMN_DATE": "date_updated",
            "FILTER_COLUMN": "cst_size",
            "FILTER_VALUE": "1",
            "CHECK": "row_count > 0",
            "NAME_OUTCOME": "positive",
        },
    )
    for check in scan_result["checks"]:
        assert check["name"].lower() == f"Row count in {table_name} must be positive".lower()
        assert check["table"].lower() == table_name.lower()
        assert "{" not in check["definition"]
        assert "}" not in check["definition"]
