import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest import mock

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.dict_helpers import assert_dict, matcher_string_contains
from helpers.mock_soda_cloud import (
    MockHttpMethod,
    MockRequest,
    MockResponse,
    MockSodaCloud,
)
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.datetime_conversions import convert_datetime_to_str
from soda_core.common.exceptions import (
    FailedContractSkeletonGenerationException,
    SodaCloudException,
)
from soda_core.common.soda_cloud import ContractSkeletonGenerationState, SodaCloud
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource
from soda_core.contracts.contract_publication import ContractPublicationResult
from soda_core.contracts.contract_verification import (
    ContractVerificationResult,
    PostProcessingStage,
    PostProcessingStageState,
)
from soda_core.contracts.impl.contract_verification_impl import (
    ContractImpl,
    ContractVerificationHandler,
    ContractVerificationHandlerRegistry,
)
from soda_core.contracts.impl.contract_yaml import ContractYaml

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("soda_cloud")
    .column_varchar("id")
    .column_integer("age")
    .rows(
        rows=[
            ("1", 1),
            (None, -1),
            ("3", None),
            ("X", 2),
        ]
    )
    .build()
)

YAML_SOURCE: SodaCloudYamlSource = SodaCloudYamlSource.from_str(
    """
soda_cloud:
  host: dev.sodadata.io
  api_key_id: some_key_id
  api_key_secret: some_key_secret
"""
)


def test_soda_cloud_from_yaml_source_with_api_key_auth():
    try:
        soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
        assert soda_cloud.api_key_id == "some_key_id"
        assert soda_cloud.api_key_secret == "some_key_secret"
        assert not soda_cloud.token
    except Exception as exc:
        pytest.fail("An unexpected exception occurred: {exc}")


def test_soda_cloud_from_yaml_source_with_token_auth():
    os.environ.update({"SODA_CLOUD_TOKEN": "some_token"})
    yaml_source = SodaCloudYamlSource.from_str(
        """
        soda_cloud:
          host: dev.sodadata.io
        """
    )
    try:
        soda_cloud = SodaCloud.from_yaml_source(yaml_source, provided_variable_values={})
        assert not soda_cloud.api_key_id
        assert not soda_cloud.api_key_secret
        assert soda_cloud.token == "some_token"
    except Exception as exc:
        pytest.fail("An unexpected exception occurred: {exc}")


def test_soda_cloud_results(data_source_test_helper: DataSourceTestHelper, env_vars: dict):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    env_vars["SODA_SCAN_ID"] = "env_var_scan_id"

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "777ggg"}),
            MockResponse(
                method=MockHttpMethod.POST,
                status_code=200,
                json_object={
                    "scanId": "ssscanid",
                    "checks": [
                        {"id": "123e4567-e89b-12d3-a456-426655440000", "identities": ["0e741893"]},
                        {"id": "456e4567-e89b-12d3-a456-426655441111", "identities": ["c12087d5"]},
                    ],
                },
            ),
        ]
    )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
              - name: age
                missing_values: [-1, -2]
                checks:
                  - missing:
                      threshold:
                        must_be_less_than_or_equal: 2
                  - missing:
                      qualifier: 2
                      name: Second missing check
                      threshold:
                        must_be_less_than_or_equal: 5
            checks:
              - schema:
        """,
    )

    request_index = 0
    request_1: MockRequest = data_source_test_helper.soda_cloud.requests[request_index]
    assert request_1.url.endswith("api/command")
    assert request_1.json["type"] == "sodaCoreUploadContractFile"

    request_index += 1
    request_2: MockRequest = data_source_test_helper.soda_cloud.requests[request_index]
    assert request_2.url.endswith("api/command")
    assert_dict(
        request_2.json,
        {
            "type": "sodaCoreInsertScanResults",
            "scanId": "env_var_scan_id",
            "checks": [
                {
                    "checkPath": "columns.age.checks.missing",
                    "name": "No missing values",
                    "diagnostics": {
                        "value": 2,
                        "fail": {"greaterThan": 2},
                        "v4": {
                            "type": "missing",
                            "failedRowsCount": 2,
                            "failedRowsPercent": 50.0,
                            "datasetRowsTested": 4,
                        },
                    },
                },
                {
                    "checkPath": "columns.age.checks.missing.2",
                    "name": "Second missing check",
                    "diagnostics": {
                        "value": 2,
                        "fail": {"greaterThan": 5},
                        "v4": {
                            "type": "missing",
                            "failedRowsCount": 2,
                            "failedRowsPercent": 50.0,
                            "datasetRowsTested": 4,
                        },
                    },
                },
                {
                    "checkPath": "checks.schema",
                },
            ],
            "postProcessingStages": [],
        },
    )
    assert (
        len(data_source_test_helper.soda_cloud.requests) == 2
    ), f"Expected 2 requests, got more: {data_source_test_helper.soda_cloud.requests}"


def test_soda_cloud_results_with_post_processing(data_source_test_helper: DataSourceTestHelper, env_vars: dict):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    class DummyHandler(ContractVerificationHandler):
        def handle(
            self,
            contract_impl: ContractImpl,
            data_source_impl: Optional[DataSourceImpl],
            contract_verification_result: ContractVerificationResult,
            soda_cloud: SodaCloud,
            soda_cloud_send_results_response_json: dict,
            dwh_data_source_file_path: Optional[str] = None,
        ):
            """
            not needed for this test
            """

        def provides_post_processing_stages(self) -> list[PostProcessingStage]:
            return [PostProcessingStage("testStage", PostProcessingStageState.ONGOING)]

    ContractVerificationHandlerRegistry.register(DummyHandler())

    env_vars["SODA_SCAN_ID"] = "env_var_scan_id"

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "777ggg"}),
            MockResponse(
                method=MockHttpMethod.POST,
                status_code=200,
                json_object={
                    "scanId": "ssscanid",
                    "checks": [
                        {"id": "123e4567-e89b-12d3-a456-426655440000", "identities": ["0e741893"]},
                        {"id": "456e4567-e89b-12d3-a456-426655441111", "identities": ["c12087d5"]},
                    ],
                },
            ),
        ]
    )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
        """,
    )

    request_2: MockRequest = data_source_test_helper.soda_cloud.requests[1]
    assert request_2.url.endswith("api/command")
    assert_dict(
        request_2.json,
        {
            "type": "sodaCoreInsertScanResults",
            "scanId": "env_var_scan_id",
            "postProcessingStages": [
                {
                    "name": "testStage",
                },
            ],
        },
    )
    assert (
        len(data_source_test_helper.soda_cloud.requests) == 2
    ), f"Expected 2 cloud requests, got more: {data_source_test_helper.soda_cloud.requests}"


def test_soda_cloud_results_with_post_processing_with_failure(
    data_source_test_helper: DataSourceTestHelper, env_vars: dict
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    class DummyHandler(ContractVerificationHandler):
        def handle(
            self,
            contract_impl: ContractImpl,
            data_source_impl: Optional[DataSourceImpl],
            contract_verification_result: ContractVerificationResult,
            soda_cloud: SodaCloud,
            soda_cloud_send_results_response_json: dict,
            dwh_data_source_file_path: Optional[str] = None,
        ):
            raise RuntimeError("Intentional failure for testing")

        def provides_post_processing_stages(self) -> list[PostProcessingStage]:
            return [PostProcessingStage("testStage", PostProcessingStageState.ONGOING)]

    ContractVerificationHandlerRegistry.register(DummyHandler())

    env_vars["SODA_SCAN_ID"] = "env_var_scan_id"

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "777ggg"}),
            MockResponse(
                method=MockHttpMethod.POST,
                status_code=200,
                json_object={
                    "scanId": "env_var_scan_id",
                    "checks": [
                        {"id": "123e4567-e89b-12d3-a456-426655440000", "identities": ["0e741893"]},
                        {"id": "456e4567-e89b-12d3-a456-426655441111", "identities": ["c12087d5"]},
                    ],
                },
            ),
            MockResponse(status_code=200, json_object={}),
            MockResponse(status_code=200, json_object={}),
        ]
    )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
        """,
    )

    request_2: MockRequest = data_source_test_helper.soda_cloud.requests[1]
    assert request_2.url.endswith("api/command")
    assert_dict(
        request_2.json,
        {
            "type": "sodaCoreInsertScanResults",
            "scanId": "env_var_scan_id",
            "postProcessingStages": [
                {
                    "name": "testStage",
                },
            ],
        },
    )

    request_3: MockRequest = data_source_test_helper.soda_cloud.requests[2]
    assert request_3.url.endswith("api/command")
    assert_dict(
        request_3.json,
        {
            "type": "sodaCorePostProcessingUpdate",
            "scanId": "env_var_scan_id",
            "name": "testStage",
            "state": "failed",
            "error": matcher_string_contains("RuntimeError: Intentional failure for testing"),
        },
    )

    assert (
        len(data_source_test_helper.soda_cloud.requests) == 3
    ), f"Expected 2 cloud requests, got more: {data_source_test_helper.soda_cloud.requests}"


def test_execute_over_agent(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(
                status_code=200,
                json_object={
                    "allowed": True,
                },
            ),
            MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": "fffileid"}),
            MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"scanId": "ssscanid"}),
            MockResponse(
                method=MockHttpMethod.GET,
                status_code=200,
                headers={"X-Soda-Next-Poll-Time": convert_datetime_to_str(datetime.now(timezone.utc))},
                json_object={
                    "scanId": "ssscanid",
                    "state": "executing",
                },
            ),
            MockResponse(
                method=MockHttpMethod.GET,
                status_code=200,
                json_object={
                    "scanId": "ssscanid",
                    "state": "completed",
                    "cloudUrl": "https://the-scan-url",
                    "contractDatasetCloudUrl": "https://the-contract-dataset-url",
                },
            ),
            MockResponse(
                method=MockHttpMethod.GET,
                status_code=200,
                json_object={
                    "content": [
                        {
                            "level": "debug",
                            "message": "m1",
                            "timestamp": "2025-02-21T06:16:58+00:00",
                            "index": 0,
                        },
                        {
                            "level": "info",
                            "message": "m2",
                            "timestamp": "2025-02-21T06:16:59+00:00",
                            "index": 1,
                        },
                    ],
                    "totalElements": 2,
                    "totalPages": 1,
                    "number": 0,
                    "size": 2,
                    "last": True,
                    "first": True,
                },
            ),
        ]
    )

    data_source_test_helper.use_agent = True

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [-1, -2]
                checks:
                  - missing:
                      threshold:
                        must_be_less_than_or_equal: 2
        """,
    )


def test_publish_contract():
    responses = [
        MockResponse(
            status_code=200,
            json_object={
                "allowed": True,
            },
        ),
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": "fake_file_id"}),
        MockResponse(
            method=MockHttpMethod.POST,
            json_object={
                "publishedContract": {
                    "checksum": "check",
                    "fileId": "fake_file_id",
                },
                "metadata": {"source": {"filePath": "yaml_string", "type": "local"}},
            },
        ),
    ]
    mock_cloud = MockSodaCloud(responses)

    res = mock_cloud.publish_contract(
        ContractYaml.parse(
            ContractYamlSource.from_str(
                f"""
            dataset: test/some/schema/CUSTOMERS
            columns:
            - name: id
        """
            )
        )
    )

    assert isinstance(res, ContractPublicationResult)

    assert res.contract.dataset_name == "CUSTOMERS"
    assert res.contract.data_source_name == "test"
    assert res.contract.dataset_prefix == ["some", "schema"]
    assert res.contract.source.local_file_path == "yaml_string"


def test_verify_contract_on_agent_permission_check():
    responses = [
        MockResponse(
            status_code=200,
            json_object={
                "allowed": False,
                "reason": "missingManageContracts",
            },
        ),
    ]
    mock_cloud = MockSodaCloud(responses)

    res = mock_cloud.verify_contract_on_agent(
        ContractYaml.parse(
            ContractYamlSource.from_str(
                f"""
            dataset: test/some/schema/CUSTOMERS
            columns:
            - name: id
        """
            )
        ),
        variables={},
        blocking_timeout_in_minutes=60,
        publish_results=False,
        verbose=False,
    )

    assert isinstance(res, ContractVerificationResult)
    assert res.sending_results_to_soda_cloud_failed is False
    assert res.contract.dataset_name == "CUSTOMERS"
    assert res.contract.data_source_name == "test"
    assert res.contract.dataset_prefix == ["some", "schema"]
    assert res.check_results == []
    assert res.measurements == []
    assert res.log_records is None


@mock.patch("requests.post")
def test_fetch_contract(mock_post):
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    mock_post.return_value = MockResponse(status_code=200, json_object={"results": [{"contents": "contract_contents"}]})

    soda_cloud.fetch_contract(dataset_identifier=DatasetIdentifier.parse("test/some/schema/CUSTOMERS"))
    mock_post.assert_called_once_with(
        url="https://dev.sodadata.io/api/query",
        headers={"User-Agent": "SodaCore/4.0.0.b1"},
        json={
            "type": "sodaCoreContracts",
            "filter": {
                "type": "and",
                "andExpressions": [
                    {
                        "type": "equals",
                        "left": {"type": "columnValue", "columnName": "identifier"},
                        "right": {"type": "string", "value": "test/some/schema/CUSTOMERS"},
                    }
                ],
            },
            "token": "some_token",
        },
    )


@mock.patch("requests.post")
def test_poll_contract_skeleton_generation__completed(mock_post):
    now = datetime.now(tz=timezone.utc)
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    mock_post.return_value = MockResponse(status_code=200, json_object={"state": "completed"})

    result_state = soda_cloud.poll_contract_skeleton_generation(
        dataset_identifier=DatasetIdentifier.parse("test/some/schema/CUSTOMERS"), blocking_timeout_in_minutes=60
    )
    mock_post.assert_called_once_with(
        url="https://dev.sodadata.io/api/query",
        headers={"User-Agent": "SodaCore/4.0.0.b1"},
        json={
            "type": "sodaCoreContractSkeletonGenerationState",
            "datasetIdentifier": "test/some/schema/CUSTOMERS",
            "lastUpdatedAfter": convert_datetime_to_str(now),
            "token": "some_token",
        },
    )
    assert result_state == ContractSkeletonGenerationState.COMPLETED


@mock.patch("requests.post")
def test_poll_contract_skeleton_generation__failed(mock_post):
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    mock_post.return_value = MockResponse(status_code=200, json_object={"state": "failed"})

    result_state = soda_cloud.poll_contract_skeleton_generation(
        dataset_identifier=DatasetIdentifier.parse("test/some/schema/CUSTOMERS"), blocking_timeout_in_minutes=60
    )
    assert result_state == ContractSkeletonGenerationState.FAILED


@mock.patch("soda_core.common.soda_cloud.datetime")
def test_poll_contract_skeleton_generation__timeout(mock_datetime):
    start_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    timeout_time = start_time + timedelta(minutes=60)
    mock_datetime.now.side_effect = [
        start_time,
        timeout_time,
    ]
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"

    with pytest.raises(FailedContractSkeletonGenerationException):
        soda_cloud.poll_contract_skeleton_generation(
            dataset_identifier=DatasetIdentifier.parse("test/some/schema/CUSTOMERS"), blocking_timeout_in_minutes=60
        )


@mock.patch("requests.post")
def test_trigger_contract_skeleton_generation__success(mock_post):
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    mock_post.return_value = MockResponse(status_code=200, json_object={"state": "completed"})

    soda_cloud.trigger_contract_skeleton_generation(
        dataset_identifier=DatasetIdentifier.parse("test/some/schema/CUSTOMERS")
    )
    mock_post.assert_called_once_with(
        url="https://dev.sodadata.io/api/command",
        headers={"User-Agent": "SodaCore/4.0.0.b1"},
        json={
            "type": "sodaCoreGenerateContractSkeleton",
            "datasetIdentifier": "test/some/schema/CUSTOMERS",
            "token": "some_token",
        },
    )


@mock.patch("requests.post")
def test_trigger_contract_skeleton_generation__error(mock_post):
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    mock_post.return_value = MockResponse(status_code=400, json_object={"message": "error message"})

    with pytest.raises(SodaCloudException):
        soda_cloud.trigger_contract_skeleton_generation(
            dataset_identifier=DatasetIdentifier.parse("test/some/schema/CUSTOMERS")
        )

    mock_post.assert_called_once_with(
        url="https://dev.sodadata.io/api/command",
        headers={"User-Agent": "SodaCore/4.0.0.b1"},
        json={
            "type": "sodaCoreGenerateContractSkeleton",
            "datasetIdentifier": "test/some/schema/CUSTOMERS",
            "token": "some_token",
        },
    )
