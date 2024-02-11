from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import CheckOutcome, ContractResult

user_defined_sql_test_table = TestTable(
    name="user_defined_sql_expr",
    columns=[("id", DataType.TEXT), ("country", DataType.TEXT)],
    # fmt: off
    values=[
        ('1', 'US'),
        ('2', 'US'),
        ('3', 'BE'),
    ]
    # fmt: on
)


def test_contract_user_defined_sql_expression(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(user_defined_sql_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
            checks:
            - type: sql_expression
              metric: us_count
              metric_sql_expression: COUNT(CASE WHEN country = 'US' THEN 1 END)
              fail_when_equal: 2
    """
    )
    assert "Actual us_count was 2" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "us_count"
    assert measurement.value == 2
    assert measurement.type == "numeric"
