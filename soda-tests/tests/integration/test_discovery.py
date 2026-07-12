from textwrap import dedent

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.test_fixtures import test_datasource
from helpers.test_table import TestTableSpecification
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.data_source import handle_discover_data_source
from soda_core.common.soda_cloud import SodaCloud
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


@pytest.mark.skipif(
    test_datasource not in {"postgres"},
    reason=(
        "System-schema exclusion is asserted against postgres' catalogs "
        "(pg_catalog/information_schema); other dialects are covered by the "
        "is_system_schema unit tests."
    ),
)
def test_discovery_excludes_system_schemas(data_source_test_helper: DataSourceTestHelper):
    """Discovery scoped to the database only (no schema prefix) must not
    return pg_catalog / information_schema datasets."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    # Database-only prefix: without the system-schema filter this returns
    # ~70 pg_catalog and information_schema tables.
    database_only_prefixes = data_source_test_helper.dataset_prefix[:1]
    dqns = DiscoveryRun.execute(data_source_test_helper.data_source_impl, prefixes=database_only_prefixes)

    # Positive control: the user table is still discovered.
    expected_suffix = test_table.unique_name.lower()
    assert any(dqn.lower().endswith(expected_suffix) for dqn in dqns), f"{test_table.unique_name} not found in {dqns}"

    system_schema_dqns = [dqn for dqn in dqns if "/pg_catalog/" in dqn.lower() or "/information_schema/" in dqn.lower()]
    assert system_schema_dqns == []


@pytest.mark.no_snapshot  # Opens its own connection from the YAML file, outside the helper's snapshot machinery.
@pytest.mark.skipif(
    test_datasource not in {"postgres"},
    reason=(
        "The handler connection lifecycle (open/close around the discovery "
        "query) is dialect-independent, so one dialect suffices. The handler "
        "discovers with unscoped prefixes, which doesn't surface the test "
        "table on some dialects (e.g. databricks/sparkdf shared metastores)."
    ),
)
def test_handle_discover_data_source_opens_connection_and_posts_payload(
    data_source_test_helper: DataSourceTestHelper, tmp_path, monkeypatch
):
    """Regression test for the `soda data-source discover` CLI path:
    from_yaml_source does not open the connection, so the handler must
    open/close it around the discovery query.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_file = tmp_path / "data_source.yml"
    data_source_file.write_text(dedent(data_source_test_helper._create_data_source_yaml_str()).strip())
    soda_cloud_file = tmp_path / "soda_cloud.yml"
    soda_cloud_file.write_text("soda_cloud:\n  host: mock.soda.io\n")

    mock_soda_cloud = MockSodaCloud([MockResponse(status_code=200, json_object={"scanId": "discovery_scan"})])
    monkeypatch.setattr(SodaCloud, "from_yaml_source", classmethod(lambda cls, *args, **kwargs: mock_soda_cloud))

    exit_code = handle_discover_data_source(
        data_source_file_path=str(data_source_file),
        include=[test_table.unique_name],
        soda_cloud_file_path=str(soda_cloud_file),
    )

    assert exit_code == ExitCode.OK
    assert len(mock_soda_cloud.requests) == 1
    posted = mock_soda_cloud.requests[0].json
    assert posted["type"] == "sodaCoreInsertScanResults"
    expected_suffix = test_table.unique_name.lower()
    assert any(
        entry["datasetQualifiedName"].lower().endswith(expected_suffix) for entry in posted["metadata"]
    ), f"{test_table.unique_name} not found in {posted['metadata']}"
