import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.common.statements.table_types import TableType
from soda_core.contracts.contract_verification import ContractVerificationResult
from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("schema")
    .column_varchar("id")
    .column_integer("size")
    .column_date("created")
    .build()
)


@pytest.mark.parametrize("table_type", [TableType.TABLE, TableType.MATERIALIZED_VIEW, TableType.VIEW])
def test_schema(data_source_test_helper: DataSourceTestHelper, table_type: TableType):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    if table_type == TableType.MATERIALIZED_VIEW:
        if not data_source_test_helper.data_source_impl.sql_dialect.supports_materialized_views():
            pytest.skip("Materialized views not supported for this dialect")
        test_table = data_source_test_helper.create_materialized_view_from_test_table(test_table)
    elif table_type == TableType.VIEW:
        if not data_source_test_helper.data_source_impl.sql_dialect.supports_views():
            pytest.skip("Views not supported for this dialect")
        test_table = data_source_test_helper.create_view_from_test_table(test_table)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

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

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    schema_diagnostics: dict = check_json["diagnostics"]["v4"]
    assert schema_diagnostics["type"] == "schema"
    assert set([c["name"] for c in schema_diagnostics["actual"]]) == {"id", "size", "created"}
    assert set([c["name"] for c in schema_diagnostics["expected"]]) == {"id", "size", "created"}


def test_schema_warn_not_supported(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
                  threshold:
                    level: warn
            columns:
              - name: id
                data_type: {test_table.data_type('id')}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    schema_diagnostics: dict = check_json["diagnostics"]["v4"]
    assert schema_diagnostics["type"] == "schema"
    assert set([c["name"] for c in schema_diagnostics["actual"]]) == {"id", "size", "created"}
    assert set([c["name"] for c in schema_diagnostics["expected"]]) == {"id"}


def test_schema_errors(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    if data_source_test_helper.data_source_impl.sql_dialect.supports_data_type_character_maximum_length():
        char_str = "character_maximum_length: 512"
        n_failures = 2
    else:
        char_str = ""
        n_failures = 1

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                data_type: {test_table.data_type('id')}
                {char_str}
              - name: sizzze
              - name: created
                data_type: {test_table.data_type('id')}
        """,
    )

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert len(schema_check_result.column_data_type_mismatches) == n_failures


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


def test_schema_metadata_query_exists(data_source_test_helper: DataSourceTestHelper):
    # We run this so we are sure the schema exists
    _ = data_source_test_helper.ensure_test_table(test_table_specification)

    schema_exists: bool = data_source_test_helper.data_source_impl.test_schema_exists(
        prefixes=data_source_test_helper.dataset_prefix
    )
    assert schema_exists == True

    schema_should_not_exist: bool = data_source_test_helper.data_source_impl.test_schema_exists(
        prefixes=data_source_test_helper.dataset_prefix[:-1] + ["not_a_schema"]
    )
    assert schema_should_not_exist == False
