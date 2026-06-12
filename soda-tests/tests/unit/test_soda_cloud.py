import json
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
    ContractNotFoundException,
    DatasetNotFoundException,
    DataSourceNotFoundException,
    FailedContractSkeletonGenerationException,
    SodaCloudException,
)
from soda_core.common.soda_cloud import (
    ContractSkeletonGenerationState,
    SodaCloud,
    _build_diagnostics_json_dict,
)
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource
from soda_core.contracts.contract_publication import ContractPublicationResult
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    ContractVerificationResult,
    PostProcessingStage,
    PostProcessingStageState,
    ScanTokenUsage,
)
from soda_core.contracts.impl.contract_verification_impl import (
    ContractImpl,
    ContractVerificationHandler,
    ContractVerificationHandlerRegistry,
)
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.contracts.impl.diagnostics_warehouse_files import (
    DiagnosticsWarehouseFiles,
)

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


def test_send_failed_rows_diagnostics_logs_warning_without_stdout(capsys):
    soda_cloud = SodaCloud(
        host="dev.sodadata.io",
        api_key_id="some_key_id",
        api_key_secret="some_key_secret",
        token=None,
        port=None,
        scheme=None,
    )

    with mock.patch("soda_core.common.soda_cloud.logger.warning") as warning_mock:
        soda_cloud.send_failed_rows_diagnostics("scan-123", [])

    captured = capsys.readouterr()
    assert captured.out == ""
    warning_mock.assert_called_once_with(
        "Skipping failed rows diagnostics upload for scan '%s': feature not implemented yet (%d diagnostic entries).",
        "scan-123",
        0,
    )


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
            dwh_files: Optional[DiagnosticsWarehouseFiles] = None,
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
            dwh_files: Optional[DiagnosticsWarehouseFiles] = None,
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
    ), f"Expected 3 cloud requests, got more: {data_source_test_helper.soda_cloud.requests}"


def test_execute_over_runner(data_source_test_helper: DataSourceTestHelper):
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

    data_source_test_helper.use_runner = True

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


def test_execute_over_runner_completed_with_warnings(data_source_test_helper: DataSourceTestHelper):
    """When the runner returns completedWithWarnings, is_warned must be True and is_passed must be False."""
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
                    "state": "completedWithWarnings",
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

    data_source_test_helper.use_runner = True

    data_source_test_helper.assert_contract_warn(
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


def test_verify_contract_on_runner_permission_check():
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

    res = mock_cloud.verify_contract_on_runner(
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
    assert res.check_collection.dataset_name == "CUSTOMERS"
    assert res.check_collection.data_source_name == "test"
    assert res.check_collection.dataset_prefix == ["some", "schema"]
    assert res.check_results == []
    assert res.measurements == []
    assert res.log_records is None


def test_verify_contract_on_agent_permission_check_deprecated():
    """Backwards-compat: the legacy ``verify_contract_on_agent`` method still works and warns."""
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

    with pytest.warns(DeprecationWarning, match="verify_contract_on_agent"):
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


@pytest.mark.parametrize(
    "threshold_value, expected_diagnostics_value",
    [
        (True, 1),
        (False, 0),
        (42, 42),
        (3.14, 3.14),
        (0, 0),
        (None, 0),
    ],
)
def test_build_diagnostics_json_dict_casts_bool_to_int(threshold_value, expected_diagnostics_value):
    check_result = CheckResult(
        check=mock.MagicMock(),
        outcome=CheckOutcome.PASSED,
        threshold_value=threshold_value,
    )
    diagnostics = _build_diagnostics_json_dict(check_result)
    assert diagnostics["value"] == expected_diagnostics_value
    assert not isinstance(diagnostics["value"], bool)


def test_build_token_usage_dicts_serialization():
    from soda_core.common.soda_cloud import _build_token_usage_dicts

    mock_result = mock.MagicMock()
    mock_result.token_usage = [
        ScanTokenUsage(
            prompt_tokens=1500,
            completion_tokens=500,
            total_tokens=2000,
            model="gpt-4o",
            operation="autopilot",
        ),
    ]
    token_dicts = _build_token_usage_dicts(mock_result)
    assert len(token_dicts) == 1
    assert token_dicts[0] == {
        "promptTokens": 1500,
        "completionTokens": 500,
        "totalTokens": 2000,
        "model": "gpt-4o",
        "operation": "autopilot",
    }


def test_build_token_usage_dicts_empty_when_no_usage():
    from soda_core.common.soda_cloud import _build_token_usage_dicts

    mock_result = mock.MagicMock()
    mock_result.token_usage = None
    assert _build_token_usage_dicts(mock_result) == []

    mock_result.token_usage = []
    assert _build_token_usage_dicts(mock_result) == []


@mock.patch("requests.post")
def test_execute_query_primitive_posts_to_query_endpoint(mock_post):
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    mock_post.return_value = MockResponse(status_code=200, json_object={"ok": True})

    query_json_dict = {"type": "someQueryType", "dataset": {"name": "x"}}
    response = soda_cloud._execute_query(query_json_dict, request_log_name="some_query")

    assert response.status_code == 200
    mock_post.assert_called_once_with(
        url="https://dev.sodadata.io/api/query",
        headers={"User-Agent": "SodaCore/4.0.0.b1"},
        json={"type": "someQueryType", "dataset": {"name": "x"}, "token": "some_token"},
    )
    # _execute_query must not mutate the caller's dict; the auth token is injected on a copy.
    assert "token" not in query_json_dict


def _query_response(status_code, json_object=None, json_raises=False):
    response = mock.MagicMock()
    response.status_code = status_code
    response.text = "raw body text"
    if json_raises:
        response.json.side_effect = json.JSONDecodeError("Expecting value", "<html>", 0)
    else:
        response.json.return_value = json_object
    return response


def _soda_cloud_with_query_response(response):
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud._execute_query = mock.MagicMock(return_value=response)
    return soda_cloud


def test_execute_dataset_query_builds_envelope_and_returns_body():
    soda_cloud = _soda_cloud_with_query_response(_query_response(200, {"contents": "yaml"}))

    body = soda_cloud.execute_dataset_query(
        query_type="sodaCoreGetSomething",
        dataset_identifier="postgres_ds/public/orders",
        request_log_name="get_something",
    )

    assert body == {"contents": "yaml"}
    args, kwargs = soda_cloud._execute_query.call_args
    assert args[0] == {
        "type": "sodaCoreGetSomething",
        "dataset": {"datasource": "postgres_ds", "prefixes": ["public"], "name": "orders"},
    }
    assert kwargs["request_log_name"] == "get_something"


def test_execute_dataset_query_maps_datasource_not_found():
    soda_cloud = _soda_cloud_with_query_response(_query_response(400, {"code": "datasource_not_found"}))
    with pytest.raises(DataSourceNotFoundException):
        soda_cloud.execute_dataset_query("sodaCoreGetSomething", "missing_ds/public/orders", "get_something")


def test_execute_dataset_query_maps_dataset_not_found():
    soda_cloud = _soda_cloud_with_query_response(_query_response(400, {"code": "dataset_not_found"}))
    with pytest.raises(DatasetNotFoundException):
        soda_cloud.execute_dataset_query("sodaCoreGetSomething", "postgres_ds/public/missing", "get_something")


def test_execute_dataset_query_maps_caller_supplied_error_code():
    soda_cloud = _soda_cloud_with_query_response(_query_response(400, {"code": "contract_not_found"}))
    with pytest.raises(ContractNotFoundException):
        soda_cloud.execute_dataset_query(
            "sodaCoreGetContract",
            "postgres_ds/public/orders",
            "get_contract",
            error_codes={"contract_not_found": ContractNotFoundException},
        )


def test_execute_dataset_query_unrecognized_400_raises_soda_cloud_exception_with_detail():
    soda_cloud = _soda_cloud_with_query_response(_query_response(400, {"code": "x", "message": "bad input"}))
    with pytest.raises(SodaCloudException) as exc:
        soda_cloud.execute_dataset_query("sodaCoreGetSomething", "postgres_ds/public/orders", "get_something")
    assert "bad input" in str(exc.value)


def test_execute_dataset_query_non_200_raises_soda_cloud_exception():
    soda_cloud = _soda_cloud_with_query_response(_query_response(500, {"message": "boom"}))
    with pytest.raises(SodaCloudException):
        soda_cloud.execute_dataset_query("sodaCoreGetSomething", "postgres_ds/public/orders", "get_something")


def test_execute_dataset_query_none_response_raises_soda_cloud_exception():
    soda_cloud = _soda_cloud_with_query_response(None)
    with pytest.raises(SodaCloudException):
        soda_cloud.execute_dataset_query("sodaCoreGetSomething", "postgres_ds/public/orders", "get_something")


def test_execute_dataset_query_non_json_error_body_raises_soda_cloud_exception_not_decode_error():
    # A gateway returning a non-JSON 502 page must surface as a clean SodaCloudException,
    # never a raw JSONDecodeError that the CLI would treat as an unexpected crash.
    soda_cloud = _soda_cloud_with_query_response(_query_response(502, json_raises=True))
    with pytest.raises(SodaCloudException):
        soda_cloud.execute_dataset_query("sodaCoreGetSomething", "postgres_ds/public/orders", "get_something")


def test_execute_dataset_query_non_json_200_body_returns_empty_dict():
    soda_cloud = _soda_cloud_with_query_response(_query_response(200, json_raises=True))
    assert soda_cloud.execute_dataset_query("sodaCoreGetSomething", "postgres_ds/public/orders", "get_something") == {}
