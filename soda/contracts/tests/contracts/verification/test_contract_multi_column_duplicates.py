from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.contracts.contract import ContractResult, CheckOutcome
from soda.execution.data_type import DataType

contracts_multi_column_duplicates_test_table = TestTable(
    name="multi_column_duplicates",
    columns=[
        ("country_code", DataType.TEXT),
        ("zip", DataType.TEXT)
    ],
    # fmt: off
    values=[
        ('BE',  "2300"),
        ('BE',  "2300"),
        ('BE',  "2300"),
        ('BE',  "3000"),
        ('NL',  "0001"),
        ('NL',  "0002"),
        ('NL',  "0003")
    ]
    # fmt: on
)


def test_contract_multi_column_duplicates(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_multi_column_duplicates_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: country_code
          - name: zip
        checks:
          - type: multi_column_duplicates
            columns: ['country_code', 'zip']
    """)
    assert "Actual duplicate_count(country_code, zip) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "duplicate_count(country_code, zip)"
    assert measurement.value == 1
    assert measurement.type == "numeric"
