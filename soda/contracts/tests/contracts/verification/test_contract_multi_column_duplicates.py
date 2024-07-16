from contracts.helpers.test_data_source import TestDataSource
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import MetricCheckResult, MultiColumnDuplicateCheck
from soda.contracts.contract import CheckOutcome, ContractResult

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


def test_contract_multi_column_no_duplicate_values(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_multi_column_duplicates_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: country_code
          - name: zip
        checks:
          - type: no_duplicate_values
            columns:
            - country_code
            - zip
    """
    )
    assert "Actual duplicate_count(country_code, zip) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, MultiColumnDuplicateCheck)
    assert check.type == "no_duplicate_values"
    assert check.metric == "duplicate_count"
    assert check.column is None
    assert list(check.columns) == ["country_code", "zip"]


def test_contract_multi_column_duplicate_count(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_multi_column_duplicates_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: country_code
          - name: zip
        checks:
          - type: duplicate_count
            columns: ['country_code', 'zip']
            must_be: 0
    """
    )
    assert "Actual duplicate_count(country_code, zip) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, MultiColumnDuplicateCheck)
    assert check.type == "duplicate_count"
    assert check.metric == "duplicate_count"
    assert check.column is None
    assert list(check.columns) == ["country_code", "zip"]


def test_contract_multi_column_duplicate_percent(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_multi_column_duplicates_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: country_code
          - name: zip
        checks:
          - type: duplicate_percent
            columns: ['country_code', 'zip']
            must_be: 0
    """
    )
    assert "Actual duplicate_percent(country_code, zip) was 14.29" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert 14.28 < float(check_result.metric_value) < 14.30

    check = check_result.check
    assert isinstance(check, MultiColumnDuplicateCheck)
    assert check.type == "duplicate_percent"
    assert check.metric == "duplicate_percent"
    assert check.column is None
    assert list(check.columns) == ["country_code", "zip"]
