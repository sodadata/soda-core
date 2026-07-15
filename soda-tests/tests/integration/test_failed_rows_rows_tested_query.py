from unittest import mock

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.common.metadata_types import SamplerType
from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO
from soda_core.common.soda_cloud_dto import SamplerType as SamplerTypeDTO
from soda_core.common.soda_cloud_dto import TestRowSamplerConfigurationDTO
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("rtq_failed_rows")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (0, 4),
            (10, 20),
            (10, 17),
        ]
    )
    .build()
)

sampling_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("rtq_sampling")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (0, 4),
            (10, 20),
            (10, 17),
            (1, 2),
            (5, 100),
        ]
    )
    .build()
)


def test_failed_rows_query_with_rows_tested_query(data_source_test_helper: DataSourceTestHelper):
    """rows_tested_query should execute and flow checkRowsTested into v4 diagnostics."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "failedRowsPercent": 2 * 100 / 3,
        "datasetRowsTested": 3,
        "checkRowsTested": 3,
    }


def test_failed_rows_query_with_rows_tested_query_percent_threshold(data_source_test_helper: DataSourceTestHelper):
    """rows_tested_query + metric: percent should evaluate percent threshold correctly."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    # 2 out of 3 rows fail → ~66.7%, threshold is 50% → should fail
    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
                  threshold:
                    metric: percent
                    must_be_less_than: 50
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "failedRowsPercent": 2 * 100 / 3,
        "datasetRowsTested": 3,
        "checkRowsTested": 3,
    }


def test_failed_rows_percent_without_rows_tested_query_emits_error(data_source_test_helper: DataSourceTestHelper):
    """metric: percent without rows_tested_query should produce a validation error."""
    errors_str: str = data_source_test_helper.assert_contract_error(
        contract_yaml_str="""
            dataset: ds/schema/dummy_table
            columns: []
            checks:
              - failed_rows:
                  query: |
                    SELECT * FROM dummy_table WHERE 1=0
                  threshold:
                    metric: percent
                    must_be_less_than: 50
        """,
    )

    assert (
        "In a 'failed_rows' check with metric 'percent' and 'query'/'keys_query', 'rows_tested_query' is required"
        in errors_str
    )


def test_failed_rows_query_without_rows_tested_query_backward_compat(data_source_test_helper: DataSourceTestHelper):
    """Without rows_tested_query, checkRowsTested should be None in v4 diagnostics (backward compat)."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {"type": "failed_rows", "failedRowsCount": 2, "datasetRowsTested": 3}


def test_failed_rows_expression_emits_check_rows_tested(data_source_test_helper: DataSourceTestHelper):
    """Expression-mode failed_rows should include checkRowsTested in v4 diagnostics."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  expression: |
                    ({end_quoted} - {start_quoted}) > 5
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "failedRowsPercent": 2 * 100 / 3,
        "datasetRowsTested": 3,
        "checkRowsTested": 3,
    }


def test_failed_rows_rows_tested_query_returns_zero(data_source_test_helper: DataSourceTestHelper):
    """rows_tested_query returning 0 should not cause division-by-zero."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    {data_source_test_helper.select_literal_query(0)}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    v4 = check_json["diagnostics"]["v4"]
    assert v4["checkRowsTested"] == 0


def test_failed_rows_rows_tested_query_returns_null(data_source_test_helper: DataSourceTestHelper):
    """rows_tested_query returning NULL should result in checkRowsTested being None."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    {data_source_test_helper.select_literal_query("NULL")}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    v4 = check_json["diagnostics"]["v4"]
    assert "checkRowsTested" not in v4


def test_failed_rows_rows_tested_query_does_not_leak_to_other_checks(
    data_source_test_helper: DataSourceTestHelper,
):
    """A failed_rows rows_tested_query must not overwrite the contract's
    dataset_rows_tested. Regression for an identity-hash collision between
    the per-check rows-tested metric and the contract row_count metric.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    # rows_tested_query returns 999 vs. actual 3 — any leak is visible.
    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - row_count:
                  threshold:
                    must_be: 3
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    {data_source_test_helper.select_literal_query(999)}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    checks = soda_core_insert_scan_results_command["checks"]
    row_count_check = next(c for c in checks if c["checkType"] == "row_count")
    failed_rows_check = next(c for c in checks if c["checkType"] == "failed_rows")

    assert row_count_check["diagnostics"]["v4"]["datasetRowsTested"] == 3
    assert row_count_check["diagnostics"]["v4"]["checkRowsTested"] == 3
    assert row_count_check["outcome"] == "pass"

    assert failed_rows_check["diagnostics"]["v4"]["datasetRowsTested"] == 3
    assert failed_rows_check["diagnostics"]["v4"]["checkRowsTested"] == 999
    # 2 failing rows vs. default must_be=0 → fail. Pinned so a regression in
    # threshold evaluation (the metric is the threshold value) shows up here
    # rather than only via assert_contract_fail (any-check-fail wildcard).
    assert failed_rows_check["outcome"] == "fail"


def test_failed_rows_rows_tested_query_percent_does_not_leak_to_other_checks(
    data_source_test_helper: DataSourceTestHelper,
):
    """Percent-path regression. The sibling percent test happens to use a
    rows_tested_query that returns the real dataset count, which masks any
    identity-collision regression. This forces a divergent value.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - row_count:
                  threshold:
                    must_be: 3
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    {data_source_test_helper.select_literal_query(4)}
                  threshold:
                    metric: percent
                    must_be_less_than: 10
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    checks = soda_core_insert_scan_results_command["checks"]
    row_count_check = next(c for c in checks if c["checkType"] == "row_count")
    failed_rows_check = next(c for c in checks if c["checkType"] == "failed_rows")

    assert row_count_check["diagnostics"]["v4"]["datasetRowsTested"] == 3
    assert row_count_check["diagnostics"]["v4"]["checkRowsTested"] == 3
    assert row_count_check["outcome"] == "pass"

    # 2 failing rows / 4 reported by rows_tested_query → 50% > 10% threshold.
    assert failed_rows_check["diagnostics"]["v4"]["datasetRowsTested"] == 3
    assert failed_rows_check["diagnostics"]["v4"]["checkRowsTested"] == 4
    assert failed_rows_check["diagnostics"]["v4"]["failedRowsPercent"] == pytest.approx(50.0)
    assert failed_rows_check["outcome"] == "fail"


def test_two_failed_rows_checks_resolve_to_distinct_metrics(
    data_source_test_helper: DataSourceTestHelper,
):
    """Two failed_rows checks with different `query` (and different
    `rows_tested_query`) must produce independent measurements — both the
    failed_rows_count metric and the rows_tested_query metric must include
    their SQL in the identity hash so they don't share an id.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    # Rows are (0,4), (10,20), (10,17). end-start = 4, 10, 7.
    #   > 5 selects 2 rows; > 8 selects 1 row. Distinct counts.
    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  qualifier: gap_over_5
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    {data_source_test_helper.select_literal_query(100)}
              - failed_rows:
                  qualifier: gap_over_8
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 8
                  rows_tested_query: |
                    {data_source_test_helper.select_literal_query(200)}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    checks = soda_core_insert_scan_results_command["checks"]
    check_by_path = {c["checkPath"]: c for c in checks}
    over_5 = next(c for path, c in check_by_path.items() if path.endswith("gap_over_5"))
    over_8 = next(c for path, c in check_by_path.items() if path.endswith("gap_over_8"))

    # Distinct failed_rows_count per check — would collapse to one value if
    # FailedRowsQueryMetricImpl ids collided.
    assert over_5["diagnostics"]["v4"]["failedRowsCount"] == 2
    assert over_8["diagnostics"]["v4"]["failedRowsCount"] == 1

    # Distinct check_rows_tested per check — would collapse if
    # RowsTestedQueryMetricImpl ids collided.
    assert over_5["diagnostics"]["v4"]["checkRowsTested"] == 100
    assert over_8["diagnostics"]["v4"]["checkRowsTested"] == 200

    # Both have > 0 failing rows vs default must_be=0 → both fail. Pinned per
    # check so a wrong outcome on either side doesn't hide behind the other
    # via assert_contract_fail (any-check-fail wildcard).
    assert over_5["outcome"] == "fail"
    assert over_8["outcome"] == "fail"


def test_failed_rows_rows_tested_query_with_expression_emits_warning(data_source_test_helper: DataSourceTestHelper):
    """rows_tested_query with expression mode (no query) should emit a warning."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  expression: |
                    ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
        """,
    )

    warnings_str = contract_verification_result.get_warnings_str()
    assert (
        "In a 'failed_rows' check, 'rows_tested_query' is only used with 'query' or 'keys_query' mode; expression mode already computes check_rows_tested automatically"
        in warnings_str
    )


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_runner",
    new_callable=mock.PropertyMock(return_value=True),
)
@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_contract_test_scan_definition_type",
    new_callable=mock.PropertyMock(return_value=True),
)
def test_failed_rows_rows_tested_query_sampling_applied(
    mocked_is_running_on_runner,
    mocked_is_contract_test_scan_definition_type,
    data_source_test_helper: DataSourceTestHelper,
):
    """When sampling is enabled, rows_tested_query should be sampled like the failed rows query."""
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_sampler(SamplerType.ABSOLUTE_LIMIT):
        pytest.skip("Sampling not supported for this data source")

    test_table = data_source_test_helper.ensure_test_table(sampling_test_table_specification)
    sample_limit = 3

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.soda_cloud.set_dataset_configuration_response(
        dataset_identifier=test_table.dataset_identifier,
        dataset_configuration_dto=DatasetConfigurationDTO(
            test_row_sampler_configuration=TestRowSamplerConfigurationDTO(
                enabled=True, test_row_sampler={"type": SamplerTypeDTO.ABSOLUTE_LIMIT, "limit": sample_limit}
            )
        ),
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    # With sampling at limit 3 on a 5-row table, checkRowsTested should reflect the sampled count
    check_rows_tested = check_json["diagnostics"]["v4"]["checkRowsTested"]
    assert (
        check_rows_tested == sample_limit
    ), f"Expected checkRowsTested={sample_limit} (sampled), got {check_rows_tested}"


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_runner",
    new_callable=mock.PropertyMock(return_value=False),
)
@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_contract_test_scan_definition_type",
    new_callable=mock.PropertyMock(return_value=False),
)
def test_failed_rows_rows_tested_query_sampling_not_applied(
    mocked_is_running_on_runner,
    mocked_is_contract_test_scan_definition_type,
    data_source_test_helper: DataSourceTestHelper,
):
    """When not running on agent, rows_tested_query should NOT be sampled even if sampler config is present."""
    test_table = data_source_test_helper.ensure_test_table(sampling_test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.soda_cloud.set_dataset_configuration_response(
        dataset_identifier=test_table.dataset_identifier,
        dataset_configuration_dto=DatasetConfigurationDTO(
            test_row_sampler_configuration=TestRowSamplerConfigurationDTO(
                enabled=True, test_row_sampler={"type": SamplerTypeDTO.ABSOLUTE_LIMIT, "limit": 3}
            )
        ),
    )

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    # Without sampling, checkRowsTested should be the full table count (5 rows)
    check_rows_tested = check_json["diagnostics"]["v4"]["checkRowsTested"]
    assert check_rows_tested == 5, f"Expected checkRowsTested=5 (no sampling), got {check_rows_tested}"
