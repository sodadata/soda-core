from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.common.sql_dialect import DBDataType
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


def get_character_maximum_length_expression(
    data_source_test_helper: DataSourceTestHelper,
    expected_length: int = 255,
    overwrite_with_expected_length: bool = False,
) -> str:
    default_character_maximum_length = data_source_test_helper.data_source_impl.sql_dialect.default_varchar_length()
    character_maximum_length = default_character_maximum_length if default_character_maximum_length else expected_length
    if overwrite_with_expected_length:
        character_maximum_length = expected_length
    return (
        f"character_maximum_length: {character_maximum_length}"
        if data_source_test_helper.data_source_impl.sql_dialect.supports_varchar_length()
        else ""
    )


def test_schema(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    character_maximum_length_expression = get_character_maximum_length_expression(data_source_test_helper)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                data_type: {test_table.data_type('id')}
                {character_maximum_length_expression}
              - name: size
                data_type: {test_table.data_type('size')}
              - name: created
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    schema_diagnostics: dict = check_json["diagnostics"]["v4"]
    assert schema_diagnostics["type"] == "schema"
    assert set([c["name"] for c in schema_diagnostics["actual"]]) == {"id", "size", "created"}
    assert set([c["name"] for c in schema_diagnostics["expected"]]) == {"id", "size", "created"}


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
                {get_character_maximum_length_expression(data_source_test_helper, 512, overwrite_with_expected_length=True)}
              - name: sizzze
              - name: created
                data_type: {test_table.data_type('id')}
        """,
    )

    if data_source_test_helper.data_source_impl.sql_dialect.supports_varchar_length():
        number_of_expected_mismatches = 2
        index_of_type_mismatch = 1
    else:
        number_of_expected_mismatches = 1
        index_of_type_mismatch = 0

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert number_of_expected_mismatches == len(schema_check_result.column_data_type_mismatches)

    length_mismatch = schema_check_result.column_data_type_mismatches[0]

    data_type_map = data_source_test_helper._get_contract_data_type_dict()

    varchar = lambda length: data_source_test_helper.data_source_impl.sql_dialect.text_col_type(length)

    default_varchar_length = data_source_test_helper.data_source_impl.sql_dialect.default_varchar_length()

    if default_varchar_length and data_source_test_helper.data_source_impl.sql_dialect.supports_varchar_length():
        assert varchar(default_varchar_length) == length_mismatch.get_actual()
        assert varchar(512) == length_mismatch.get_expected()
    elif data_source_test_helper.data_source_impl.sql_dialect.supports_varchar_length():
        assert varchar(255) == length_mismatch.get_actual()
        assert varchar(512) == length_mismatch.get_expected()

    type_mismatch = schema_check_result.column_data_type_mismatches[index_of_type_mismatch]
    expected_data_type = data_source_test_helper.data_source_impl.sql_dialect.add_data_type_default_length(
        data_type_map[DBDataType.DATE]
    )
    assert expected_data_type == type_mismatch.get_actual()
    assert data_type_map[DBDataType.TEXT] == type_mismatch.get_expected()


def test_schema_default_order(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
              - name: created
              - name: size
        """,
    )

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert schema_check_result.are_columns_out_of_order


def test_schema_allow_out_of_order(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
                  allow_other_column_order: true
            columns:
              - name: id
              - name: created
              - name: size
        """,
    )

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert schema_check_result.are_columns_out_of_order == False


def test_schema_extra_columns_default(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
              - name: size
        """,
    )

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert schema_check_result.actual_column_names_not_expected == ["created"]


def test_schema_allow_extra_columns(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
                  allow_extra_columns: true
            columns:
              - name: id
              - name: size
        """,
    )

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert schema_check_result.actual_column_names_not_expected == []
