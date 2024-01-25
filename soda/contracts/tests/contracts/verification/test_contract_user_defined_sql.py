from contracts.helpers.contract_test_tables import contracts_test_table, contract_refs_test_table
from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.contracts.contract import ContractResult, CheckOutcome
from soda.execution.data_type import DataType

user_defined_sql_test_table = TestTable(
    name="user_defined_sql",
    columns=[
        ("id", DataType.TEXT),
        ("country", DataType.TEXT)
    ],
    # fmt: off
    values=[
        ('1', 'US'),
        ('2', 'US'),
        ('3', 'BE'),
    ]
    # fmt: on
)

def test_contract_row_count(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(user_defined_sql_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
        checks:
          - type: user_defined_sql
            metric: us_count
            query: |
              SELECT COUNT(*)
              FROM {table_name}
              WHERE country = 'US'
            fail_when_between: [0, 5]
    """)
    assert "Actual us_count was 2.0" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "us_count"
    assert measurement.value == 2
    assert measurement.type == "numeric"
