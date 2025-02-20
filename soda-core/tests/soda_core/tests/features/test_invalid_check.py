from soda_core.contracts.contract_verification import ContractResult, CheckOutcome, NumericDiagnostic, Diagnostic
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid")
    .column_text("id")
    .column_integer("age")
    .rows(rows=[
        ("1",  1),
        (None, -1),
        ("3",  None),
        ("X",  2),
    ])
    .build()
)


def test_invalid_count(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_values: ['1', '2', '3']
                checks:
                  - type: invalid_count
        """
    )
    diagnostic: Diagnostic = contract_result.check_results[0].diagnostics[0]
    assert isinstance(diagnostic, NumericDiagnostic)
    assert "invalid_count" == diagnostic.name
    assert 1 == diagnostic.value


def test_invalid_count_valid_regex_sql(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_regex_sql: ^[123]$
                checks:
                  - type: invalid_count
        """
    )
    diagnostic: Diagnostic = contract_result.check_results[0].diagnostics[0]
    assert isinstance(diagnostic, NumericDiagnostic)
    assert "invalid_count" == diagnostic.name
    assert 1 == diagnostic.value


def test_invalid_count_valid_min_max(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                valid_min: 1
                valid_max: 2
                checks:
                  - type: invalid_count
        """
    )
    diagnostic: Diagnostic = contract_result.check_results[0].diagnostics[0]
    assert isinstance(diagnostic, NumericDiagnostic)
    assert "invalid_count" == diagnostic.name
    assert 1 == diagnostic.value


def test_invalid_count_invalid_regex_sql(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                invalid_regex_sql: ^[X]$
                checks:
                  - type: invalid_count
        """
    )
    diagnostic: Diagnostic = contract_result.check_results[0].diagnostics[0]
    assert isinstance(diagnostic, NumericDiagnostic)
    assert "invalid_count" == diagnostic.name
    assert 1 == diagnostic.value


def test_invalid_count_valid_format(data_source_test_helper: DataSourceTestHelper):

    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/701311a4-6fc3-4f41-86a1-2a7fe4dc358f/checks

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    # This test depends on data source configuration 'format_regexes'
    # For example, see soda-postgres/tests/soda_postgres/contracts/impl/data_sources/postgres_data_source_test_helper.py
    #             "format_regexes": {
    #                 "single_digit_test_format": "^[0-9]$"
    #             }

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_format: single_digit_test_format
                checks:
                  - type: invalid_count
        """
    )
    diagnostic: Diagnostic = contract_result.check_results[0].diagnostics[0]
    assert isinstance(diagnostic, NumericDiagnostic)
    assert "invalid_count" == diagnostic.name
    assert 1 == diagnostic.value
