import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.common.metadata_types import SqlDataType
from soda_core.common.statements.table_types import TableType
from soda_core.contracts.contract_verification import ContractVerificationResult
from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("schema")
    .column_varchar("id")
    .column_integer("size")
    .column_date("created")
    .column_varchar("label", character_maximum_length=100)
    .column_numeric("score", numeric_precision=10, numeric_scale=2)
    .column_timestamp("created_at", datetime_precision=3)
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
            MockResponse(
                status_code=200,
                json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"},
            ),
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
              - name: label
                data_type: {test_table.data_type('label')}
              - name: score
                data_type: {test_table.data_type('score')}
              - name: created_at
                data_type: {test_table.data_type('created_at')}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    schema_diagnostics: dict = check_json["diagnostics"]["v4"]
    assert schema_diagnostics["type"] == "schema"
    assert set([c["name"] for c in schema_diagnostics["actual"]]) == {
        "id",
        "size",
        "created",
        "label",
        "score",
        "created_at",
    }
    assert set([c["name"] for c in schema_diagnostics["expected"]]) == {
        "id",
        "size",
        "created",
        "label",
        "score",
        "created_at",
    }


def test_schema_warn_not_supported(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(
                status_code=200,
                json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"},
            ),
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
    assert set([c["name"] for c in schema_diagnostics["actual"]]) == {
        "id",
        "size",
        "created",
        "label",
        "score",
        "created_at",
    }
    assert set([c["name"] for c in schema_diagnostics["expected"]]) == {"id"}


def test_schema_errors(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect

    if sql_dialect.supports_data_type_character_maximum_length():
        char_str = "character_maximum_length: 512"
        # The id column has no explicit length, so actual length is None.
        # Some dialects (e.g. Trino) use permissive comparison that skips
        # the length check when actual is None, so the mismatch isn't detected.
        length_mismatch_detected = not sql_dialect.is_same_data_type_for_schema_check(
            expected=SqlDataType(name="varchar", character_maximum_length=512),
            actual=SqlDataType(name="varchar", character_maximum_length=None),
        )
        n_failures = 2 if length_mismatch_detected else 1
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
              - name: label
                data_type: {test_table.data_type('label')}
              - name: score
                data_type: {test_table.data_type('score')}
              - name: created_at
                data_type: {test_table.data_type('created_at')}
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
              - name: label
              - name: score
              - name: created_at
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
              - name: label
              - name: score
              - name: created_at
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
    assert schema_check_result.actual_column_names_not_expected == ["created", "label", "score", "created_at"]


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


def test_schema_precision_pass(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect

    # Use actual DB metadata values — dialects may adjust precision from what was requested
    # in the table spec (e.g. Trino normalizes datetime_precision=3 to 6).
    actual_by_name = data_source_test_helper.get_actual_column_metadata(test_table)

    n_precision_fields_tested = 0

    label_extras = ""
    actual_label = actual_by_name["label"].sql_data_type
    if actual_label.character_maximum_length is not None:
        label_extras = f"character_maximum_length: {actual_label.character_maximum_length}"
        n_precision_fields_tested += 1

    score_extras = ""
    actual_score = actual_by_name["score"].sql_data_type
    if actual_score.numeric_precision is not None:
        score_extras = f"numeric_precision: {actual_score.numeric_precision}"
        n_precision_fields_tested += 1
        if actual_score.numeric_scale is not None:
            score_extras += f"\n                numeric_scale: {actual_score.numeric_scale}"

    ts_extras = ""
    actual_ts = actual_by_name["created_at"].sql_data_type
    if actual_ts.datetime_precision is not None:
        ts_extras = f"datetime_precision: {actual_ts.datetime_precision}"
        n_precision_fields_tested += 1

    if n_precision_fields_tested == 0:
        pytest.skip("Dialect does not return any precision metadata")

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
              - name: label
                data_type: {test_table.data_type('label')}
                {label_extras}
              - name: score
                data_type: {test_table.data_type('score')}
                {score_extras}
              - name: created_at
                data_type: {test_table.data_type('created_at')}
                {ts_extras}
        """,
    )


def test_schema_precision_mismatch(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect

    # Use actual DB metadata — dialects may adjust precision from what was requested.
    # Derive "wrong" values from actuals to guarantee they differ.
    actual_by_name = data_source_test_helper.get_actual_column_metadata(test_table)
    n_expected_mismatches = 0

    label_extras = ""
    actual_label = actual_by_name["label"].sql_data_type
    if actual_label.character_maximum_length is not None:
        wrong_length = actual_label.character_maximum_length + 100
        label_extras = f"character_maximum_length: {wrong_length}"
        length_mismatch_detected = not sql_dialect.is_same_data_type_for_schema_check(
            expected=SqlDataType(name=actual_label.name, character_maximum_length=wrong_length),
            actual=actual_label,
        )
        if length_mismatch_detected:
            n_expected_mismatches += 1

    score_extras = ""
    actual_score = actual_by_name["score"].sql_data_type
    if actual_score.numeric_precision is not None:
        wrong_precision = actual_score.numeric_precision + 5
        score_extras = f"numeric_precision: {wrong_precision}"
        wrong_scale = None
        if actual_score.numeric_scale is not None:
            wrong_scale = actual_score.numeric_scale + 3
            score_extras += f"\n                numeric_scale: {wrong_scale}"
        numeric_mismatch_detected = not sql_dialect.is_same_data_type_for_schema_check(
            expected=SqlDataType(name=actual_score.name, numeric_precision=wrong_precision, numeric_scale=wrong_scale),
            actual=actual_score,
        )
        if numeric_mismatch_detected:
            n_expected_mismatches += 1

    ts_extras = ""
    actual_ts = actual_by_name["created_at"].sql_data_type
    if actual_ts.datetime_precision is not None:
        wrong_dt_precision = actual_ts.datetime_precision + 1
        ts_extras = f"datetime_precision: {wrong_dt_precision}"
        datetime_mismatch_detected = not sql_dialect.is_same_data_type_for_schema_check(
            expected=SqlDataType(name=actual_ts.name, datetime_precision=wrong_dt_precision),
            actual=actual_ts,
        )
        if datetime_mismatch_detected:
            n_expected_mismatches += 1

    if n_expected_mismatches == 0:
        pytest.skip("Dialect does not support any precision types")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
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
              - name: label
                data_type: {test_table.data_type('label')}
                {label_extras}
              - name: score
                data_type: {test_table.data_type('score')}
                {score_extras}
              - name: created_at
                data_type: {test_table.data_type('created_at')}
                {ts_extras}
        """,
    )

    schema_check_result: SchemaCheckResult = contract_verification_result.check_results[0]
    assert len(schema_check_result.column_data_type_mismatches) == n_expected_mismatches


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
