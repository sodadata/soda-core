"""
A check-collection subtype whose results never reference the uploaded YAML file sets
``uploads_yaml_file = False``. Publishing then skips the file upload and sends the
results straight away, on both the per-file and the combined upload path.
"""

import duckdb
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.check_collections.session import execute_check_collections
from soda_core.cli.exit_codes import ExitCode, session_result_to_exit_code
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml

_CONTRACT_YAML = """
dataset: test_ds/main/my_table
checks:
  - row_count:
"""


@pytest.fixture
def data_source_impl(tmp_path) -> DataSourceImpl:
    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE my_table (id VARCHAR)")
    connection.execute("INSERT INTO my_table VALUES ('a')")
    connection.close()
    data_source_yaml = (
        "type: duckdb\n" "name: test_ds\n" "connection:\n" f'    database: "{db_path}"\n' "    schema: main\n"
    )
    return DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(data_source_yaml))


@pytest.fixture(params=[False, True], ids=["per-file", "combined"])
def subtype_without_file_upload(request, monkeypatch) -> None:
    monkeypatch.setattr(ContractImpl, "uploads_yaml_file", False)
    monkeypatch.setattr(ContractImpl, "wire_source", "test-subtype")
    monkeypatch.setattr(ContractYaml, "columns_required", False)
    monkeypatch.setattr(ContractImpl, "combine_uploads", request.param)


def _request_types(mock_cloud: MockSodaCloud) -> list[str]:
    return [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]


def test_publish_sends_results_without_uploading_the_yaml_file(
    monkeypatch, data_source_impl, subtype_without_file_upload
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_cloud = MockSodaCloud(
        responses=[MockResponse(status_code=200, json_object={"scanId": "scan-1", "datasetId": "dataset-1"})]
    )

    def fail_upload(*args, **kwargs):
        raise AssertionError("the YAML file must not be uploaded")

    mock_cloud._upload_contract_yaml_file = fail_upload

    session_result = execute_check_collections(
        yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
        data_source_impl=None,
        soda_cloud_impl=mock_cloud,
        publish_results=True,
        all_data_source_impls={data_source_impl.name: data_source_impl},
        default_impl_class=ContractImpl,
    )

    insert_requests = [
        r.json for r in mock_cloud.requests if r.json and r.json.get("type") == "sodaCoreInsertScanResults"
    ]
    assert len(insert_requests) == 1, f"Requests seen: {_request_types(mock_cloud)}"
    assert insert_requests[0].get("contract") is None
    assert session_result.results[0].check_collection.source.soda_cloud_file_id is None
    assert session_result.results[0].sending_results_to_soda_cloud_failed is False
    assert session_result_to_exit_code(session_result) == ExitCode.OK


def test_a_file_whose_verify_raised_is_not_sent(monkeypatch, data_source_impl, subtype_without_file_upload):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    verify = ContractImpl.verify

    def verify_or_raise(self):
        if "# broken" in self.yaml.yaml_source.yaml_str_original:
            raise RuntimeError("verify blew up")
        return verify(self)

    monkeypatch.setattr(ContractImpl, "verify", verify_or_raise)
    mock_cloud = MockSodaCloud(
        responses=[MockResponse(status_code=200, json_object={"scanId": "scan-1", "datasetId": "dataset-1"})]
    )
    sent_results = []
    send_check_collection_results = mock_cloud.send_check_collection_results

    def recording_send(results, **kwargs):
        sent_results.extend(results)
        return send_check_collection_results(results, **kwargs)

    mock_cloud.send_check_collection_results = recording_send

    session_result = execute_check_collections(
        yaml_sources=[
            ContractYamlSource.from_str(_CONTRACT_YAML),
            ContractYamlSource.from_str(_CONTRACT_YAML + "# broken"),
        ],
        data_source_impl=None,
        soda_cloud_impl=mock_cloud,
        publish_results=True,
        all_data_source_impls={data_source_impl.name: data_source_impl},
        default_impl_class=ContractImpl,
    )

    assert session_result.results[1].error is not None
    assert sent_results == [session_result.results[0]]
