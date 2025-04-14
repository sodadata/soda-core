from soda_core.contracts.contract_verification import (
    ContractVerificationResult,
    Diagnostic,
    NumericDiagnostic,
)
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

referencing_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referencing")
    .column_integer("id")
    .column_text("country")
    .column_text("zip")
    .rows(
        rows=[
            (1, "NL", "NL4775"),
            (2, "NL", "XXXXXX"),
            (1, "XX", "NL4775"),
            (3, "BE", "NL4775"),
            (4, "XX", "XXXXXX"),
            (5, "XX", None),
            (6, None, "XXXXXX"),
            (7, None, None),
        ]
    )
    .build()
)


referenced_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referenced")
    .column_text("country_code")
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
                  dataset: {referenced_table.unique_name}
                  column: country_code
                checks:
                  - invalid:
        """,
    )
    diagnostic: Diagnostic = contract_verification_result.check_results[0].diagnostics[0]
    assert isinstance(diagnostic, NumericDiagnostic)
    assert "invalid_count" == diagnostic.name
    assert 3 == diagnostic.value


def test_invalid_count_excl_missing(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {referenced_table.unique_name}
                  column: country_code
                missing_values: [XX]
                checks:
                  - invalid:
        """,
    )
    diagnostic: Diagnostic = contract_verification_result.check_results[0].diagnostics[0]
    assert isinstance(diagnostic, NumericDiagnostic)
    assert "invalid_count" == diagnostic.name
    assert 0 == diagnostic.value
