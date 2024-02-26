from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import CheckOutcome, ContractResult, NumericMetricCheckResult, NumericMetricCheck, \
    DuplicateCheck

contracts_multi_column_duplicates_test_table = TestTable(
    name="multi_column_duplicates",
    columns=[("country_code", DataType.TEXT), ("zip", DataType.TEXT)],
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

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: country_code
          - name: zip
        checks:
          - type: no_duplicate_values
            columns: ['country_code', 'zip']
    """
    )
    assert "Actual duplicate_count(country_code, zip) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, DuplicateCheck)
    assert check.type == "no_duplicate_values"
    assert check.metric == "duplicate_count"
    assert check.dataset == table_name
    assert check.column is None
    assert list(check.columns) == ['country_code', 'zip']
