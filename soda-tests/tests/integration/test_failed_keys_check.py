"""
Integration tests for the `failed_rows` `keys_query` form (soda-core side).

The keys_query form counts failing rows exactly like the `query` form, keeps `datasetRowsTested`
(dataset metadata count), and supports the optional `rows_tested_query` for `checkRowsTested`.
soda-core also validates — independent of any DWH config — that an explicit `key_column_mapping`
references columns the keys_query actually outputs.

Routing the keys into the shared diagnostics-warehouse fk_/fr_ tables is exercised on the
soda-extensions side; here we only assert the soda-core behaviour, which must work with no DWH config.
"""

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("failed_keys")
    .column_integer("id")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (1, 0, 4),
            (2, 10, 20),
            (3, 10, 17),
        ]
    )
    .build()
)


def test_failed_keys_query_counts_failures_and_keeps_dataset_rows_tested(
    data_source_test_helper: DataSourceTestHelper,
):
    """keys_query counts failing rows like the query form and keeps datasetRowsTested (no suppression)."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")
    id_quoted = data_source_test_helper.quote_column("id")

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
                  keys_query: |
                    SELECT {id_quoted}
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {"type": "failed_rows", "failedRowsCount": 2, "datasetRowsTested": 3}


def test_failed_keys_query_with_rows_tested_query(data_source_test_helper: DataSourceTestHelper):
    """An optional rows_tested_query flows checkRowsTested into v4 diagnostics, like the query form."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")
    id_quoted = data_source_test_helper.quote_column("id")

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
                  keys_query: |
                    SELECT {id_quoted}
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


def test_failed_keys_query_with_mapping_is_accepted_and_counts(data_source_test_helper: DataSourceTestHelper):
    """soda-core accepts a key_column_mapping and still counts like the query form. The mapping is
    inert without DWH config (key routing/column-existence checking happens in soda-extensions), so
    here we only assert the form runs and counts with a mapping present."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")
    id_quoted = data_source_test_helper.quote_column("id")

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
                  keys_query: |
                    SELECT {id_quoted}
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
                  key_column_mapping:
                    customer_id: id
        """,
    )

    check_json: dict = data_source_test_helper.soda_cloud.requests[1].json["checks"][0]
    assert check_json["diagnostics"]["v4"]["failedRowsCount"] == 2
