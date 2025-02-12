from soda_core.contracts.contract_verification import ContractResult
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification


referencing_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referencing")
    .column_integer("id")
    .column_text("country")
    .column_text("zip")
    .rows(rows=[
        (1, "NL", "NL4775"),
        (2, "NL", "XXXXXX"),
        (1, "XX", "NL4775"),
        (3, "BE", "NL4775"),
        (4, "XX", "XXXXXX"),
        (5, "XX", None),
        (6, None, "XXXXXX"),
        (7, None, None),
    ])
    .build()
)


referenced_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referenced")
    .column_text("country_code")
    .column_text("zip_code")
    .rows(rows=[
        ("NL", "NL4775"),
        ("BE", "2300"),
    ])
    .build()
)


def test_invalid_count(data_source_test_helper: DataSourceTestHelper):

    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/2945ba9d-b1ff-4cfd-b277-d5e4edfa2bd5/checks

    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {referenced_table.unique_name}
                  column: country_code
                checks:
                  - type: invalid_count
        """
    )
    diagnostic_line: str = contract_result.check_results[0].diagnostic_lines[0]
    assert "Actual invalid_count was 3" in diagnostic_line
