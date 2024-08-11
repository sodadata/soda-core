from contracts.helpers.contract_parse_errors import get_parse_errors_str
from contracts.helpers.contract_data_source_test_helper import ContractDataSourceTestHelper
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import MetricCheck, MetricCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult

contracts_invalid_test_table = TestTable(
    name="contracts_invalid",
    # fmt: off
    columns=[
        ("one", DataType.TEXT)
    ],
    values=[
        ('ID1',),
        ('XXX',),
        ('N/A',),
        (None,),
    ]
    # fmt: on
)


def test_contract_no_invalid_with_valid_values_pass(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_invalid_test_table,
        contract_yaml_str=f"""
            columns:
              - name: one
                checks:
                - type: no_invalid_values
                  valid_length: 3
        """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 0

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "no_invalid_values"
    assert check.metric == "invalid_count"
    assert check.column.lower() == "one"


def test_contract_no_invalid_with_valid_values_fail(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_invalid_test_table,
        contract_yaml_str=f"""
        columns:
          - name: one
            checks:
            - type: no_invalid_values
              valid_values: ['ID1']
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "no_invalid_values"
    assert check.metric == "invalid_count"
    assert check.column.lower() == "one"

    assert "actual invalid_count(one) was 2" in str(contract_result).lower()


def test_no_invalid_with_threshold():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
              checks:
                - type: no_invalid_values
                  valid_values: ['ID1']
                  must_be: 0
        """
    )

    assert "Check type 'no_invalid_values' does not allow for threshold keys must_..." in errors_str


def test_no_invalid_without_valid_configuration():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
              checks:
                - type: no_invalid_values
        """
    )

    assert "Check type 'no_invalid_values' must have a validity configuration like" in errors_str


def test_contract_invalid_count_pass(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_invalid_test_table,
        contract_yaml_str=f"""
        columns:
          - name: one
            checks:
              - type: invalid_count
                valid_values: ['ID1']
                must_be: 2
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "invalid_count"
    assert check.metric == "invalid_count"
    assert check.column.lower() == "one"


def test_contract_invalid_count_fail(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_invalid_test_table,
        contract_yaml_str=f"""
        columns:
          - name: one
            checks:
              - type: invalid_count
                valid_values: ['ID1']
                must_be: 0
    """
    )
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "invalid_count"
    assert check.metric == "invalid_count"
    assert check.column.lower() == "one"

    assert "actual invalid_count(one) was 2" in str(contract_result).lower()


def test_contract_missing_and_invalid_values_pass(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_invalid_test_table,
        contract_yaml_str=f"""
        columns:
          - name: one
            checks:
              - type: missing_count
                missing_values: ['N/A']
                must_be: 2
              - type: invalid_count
                valid_values: ['ID1']
                must_be: 1
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "missing_count"
    assert check.metric == "missing_count"
    assert check.column.lower() == "one"

    check_result = contract_result.check_results[2]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "invalid_count"
    assert check.metric == "invalid_count"
    assert check.column.lower() == "one"


contracts_invalid_multi_test_table = TestTable(
    name="contracts_missing_multi",
    # fmt: off
    columns=[
        ("one", DataType.TEXT)
    ],
    values=[
        ('ID1',),
        ('XXX',),
        ('N/A',),
        ('1234567890',),
        (None,),
    ]
    # fmt: on
)


def test_contract_multi_validity_configs(data_source_test_helper: ContractDataSourceTestHelper):
    # AND logic is applied between all the specified validity configs
    # So ALL of the validity constraints have to be met
    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_invalid_multi_test_table,
        contract_yaml_str=f"""
        columns:
          - name: one
            checks:
              - type: invalid_count
                valid_values: ['ID1', 'XXX', '1234567890' ]
                valid_max_length: 4
                must_be: 2
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "invalid_count"
    assert check.metric == "invalid_count"
    assert check.column.lower() == "one"


contract_reference_test_table = TestTable(
    name="contract_reference",
    # fmt: off
    columns=[
        ("id", DataType.TEXT),
        ("ref_id", DataType.TEXT)
    ],
    values=[
        ('1', 'ID1'),
        ('2', 'ID-BUZZZ'),
        ('2', 'Undefined'),
        ('3', None),
    ]
    # fmt: on
)


def test_contract_column_invalid_reference_check(data_source_test_helper: ContractDataSourceTestHelper):
    reference_data_table_name: str = data_source_test_helper.ensure_test_table(contracts_invalid_test_table)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contract_reference_test_table,
        contract_yaml_str=f"""
        columns:
          - name: id
          - name: ref_id
            checks:
              - type: no_invalid_values
                valid_values_reference_data:
                    dataset: {reference_data_table_name}
                    column: one
                samples_limit: 20
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "no_invalid_values"
    assert check.metric == "invalid_count"
    assert check.column.lower() == "ref_id"

    assert "actual invalid_count(ref_id) was 2" in str(contract_result).lower()
