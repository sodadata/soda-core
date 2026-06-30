# soda-tests/tests/integration/test_discovery.py
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.discovery.discovery_payload import (
    build_discovery_payload,
    send_discovery_results,
)
from soda_core.discovery.discovery_run import DiscoveryRun

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("discovery")
    .column_varchar("id")
    .column_integer("age")
    .rows(rows=[("1", 30), ("2", 40)])
    .build()
)


def test_discovery_posts_dqn_only_v4_payload(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"scanId": "discovery_scan"})]
    )

    # Scope discovery to the test schema for determinism (shared CI DB has many schemas).
    prefixes = data_source_test_helper._create_dataset_prefix()
    dqns = DiscoveryRun.execute(data_source_test_helper.data_source_impl, prefixes=prefixes)

    payload = build_discovery_payload(
        dqns=dqns,
        data_source_name=data_source_test_helper.data_source_impl.name,
        scan_definition_name=f"{data_source_test_helper.data_source_impl.name}_schema_discovery_scan",
    )
    send_discovery_results(data_source_test_helper.soda_cloud, payload)

    posted = data_source_test_helper.soda_cloud.requests[0].json
    assert posted["type"] == "sodaCoreInsertScanResults"
    assert posted["version"] == "4"
    assert all(set(entry.keys()) == {"datasetQualifiedName"} for entry in posted["metadata"])
    expected_suffix = test_table.unique_name.lower()
    assert any(
        entry["datasetQualifiedName"].lower().endswith(expected_suffix) for entry in posted["metadata"]
    ), f"{test_table.unique_name} not found in {posted['metadata']}"
