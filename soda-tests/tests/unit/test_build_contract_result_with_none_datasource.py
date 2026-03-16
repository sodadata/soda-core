"""
Regression test for SAS-10945: when a data source name in the contract YAML
does not match any registered data source, data_source_impl ends up as None.
Publishing results to Soda Cloud must not crash with an AttributeError on
`data_source.name`.
"""

from datetime import datetime, timezone

from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.common.soda_cloud import _build_contract_result_json_dict
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    Contract,
    ContractVerificationResult,
    ContractVerificationSession,
    ContractVerificationStatus,
    YamlFileContentInfo,
)


def _make_contract_verification_result(data_source=None) -> ContractVerificationResult:
    """Build a minimal ContractVerificationResult with an optional (possibly None) data_source."""
    now = datetime.now(tz=timezone.utc)
    return ContractVerificationResult(
        contract=Contract(
            data_source_name=None,
            dataset_prefix=["some", "schema"],
            dataset_name="my_table",
            soda_qualified_dataset_name="missing_ds/some/schema/my_table",
            source=YamlFileContentInfo(
                source_content_str="dataset: missing_ds/some/schema/my_table\ncolumns:\n  - name: id\n",
                local_file_path="contract.yml",
                soda_cloud_file_id="fake_file_id",
            ),
        ),
        data_source=data_source,
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=ContractVerificationStatus.ERROR,
        measurements=[],
        check_results=[],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
    )


def test_build_contract_result_json_dict_does_not_crash_when_data_source_is_none():
    result = _make_contract_verification_result(data_source=None)
    json_dict = _build_contract_result_json_dict(result)

    assert isinstance(json_dict, dict)
    # to_jsonnable strips None values, so these keys should be absent
    assert "defaultDataSource" not in json_dict
    assert "defaultDataSourceProperties" not in json_dict
    assert json_dict["hasErrors"] is True


def test_verify_does_not_send_results_to_cloud_when_datasource_not_found():
    """
    When the data source name in the contract's 'dataset' field
    does not match any registered data source, verify() must:
    - NOT attempt to send results to Soda Cloud (no send_contract_result call)
    - Mark sending_results_to_soda_cloud_failed as True
    - Not crash
    """
    mock_cloud = MockSodaCloud(
        [
            # Response for the contract YAML file upload
            MockResponse(status_code=200, json_object={"fileId": "777ggg"}),
            # Response for send_contract_result — should NOT be consumed
            MockResponse(status_code=200, json_object={"scanId": "should_not_be_reached"}),
        ]
    )

    # Contract references data source "wrong_ds_name" but no data source with that name is registered.
    contract_yaml_str = """
        dataset: wrong_ds_name/some/schema/my_table
        columns:
          - name: id
    """

    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract_yaml_str)],
        data_source_impls=[],  # No data sources registered at all
        soda_cloud_impl=mock_cloud,
        soda_cloud_publish_results=True,
    )

    assert len(session_result.contract_verification_results) == 1
    result = session_result.contract_verification_results[0]

    # Results should NOT have been sent to Cloud
    assert result.sending_results_to_soda_cloud_failed is True

    # Only the file upload request should have been made, NOT send_contract_result
    assert len(mock_cloud.requests) == 1
    assert mock_cloud.requests[0].json["type"] == "sodaCoreUploadContractFile"
