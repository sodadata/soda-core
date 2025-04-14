from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult
from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("schema")
    .column_text("id")
    .column_integer("size")
    .column_date("created")
    .build()
)


def test_schema(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                data_type: {test_table.data_type('id')}
              - name: size
                data_type: {test_table.data_type('size')}
              - name: created
        """,
    )


def test_schema_errors(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                data_type: {test_table.data_type('id')}
                character_maximum_length: 512
              - name: sizzze
              - name: created
                data_type: {test_table.data_type('id')}
        """,
    )

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert 2 == len(schema_check_result.column_data_type_mismatches)

    length_mismatch = schema_check_result.column_data_type_mismatches[0]
    assert "character varying(255)" == length_mismatch.get_actual()
    assert "varchar(512)" == length_mismatch.get_expected()

    type_mismatch = schema_check_result.column_data_type_mismatches[1]
    assert "date" == type_mismatch.get_actual()
    assert "varchar" == type_mismatch.get_expected()
