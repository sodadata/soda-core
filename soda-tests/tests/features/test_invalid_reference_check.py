from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

referencing_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referencing")
    .column_integer("id")
    .column_varchar("country", 2)
    .column_text("zip")
    .rows(
        rows=[
            (1, "NL", "NL4775"),
            (2, "NL", "XXXXXX"),
            (3, "XX", "NL4775"),
            (4, "BE", "NL4775"),
            (5, "XX", "XXXXXX"),
            (6, "XX", None),
            (7, None, "XXXXXX"),
            (8, None, None),
        ]
    )
    .build()
)


referenced_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referenced")
    .column_varchar("country_code", 2)
    .column_text("zip_code")
    .rows(
        rows=[
            ("NL", "NL4775"),
            ("BE", "2300"),
        ]
    )
    .build()
)


def test_invalid_count(data_source_test_helper: DataSourceTestHelper):
    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/2945ba9d-b1ff-4cfd-b277-d5e4edfa2bd5/checks

    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 3


def test_invalid_count_with_check_filter(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    zip_quoted: str = data_source_test_helper.quote_column("zip")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
                      filter: |
                        {zip_quoted} = 'NL4775'
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 1
    assert get_diagnostic_value(check_result, "check_rows_tested") == 3


def test_invalid_count_with_check_and_dataset_filter(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    zip_quoted: str = data_source_test_helper.quote_column("zip")
    id_quoted: str = data_source_test_helper.quote_column("id")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            filter: |
              {id_quoted} > 1
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
                      filter: |
                        {zip_quoted} = 'NL4775'
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 1
    assert get_diagnostic_value(check_result, "check_rows_tested") == 2


def test_invalid_count_excl_missing(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                missing_values: [XX]
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 0
    )
