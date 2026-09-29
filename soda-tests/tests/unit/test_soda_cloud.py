import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest import mock

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.dict_helpers import assert_dict, matcher_string_contains
from helpers.mock_soda_cloud import MockHttpMethod, MockRequest, MockResponse, MockSodaCloud
from helpers.test_table import TestTableSpecification
from soda_core.__version__ import SODA_CORE_VERSION
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import interpret_contract_verification_result
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.datetime_conversions import convert_datetime_to_str
from soda_core.common.exceptions import (
    ContractNotFoundException,
    DatasetNotFoundException,
    DataSourceNotFoundException,
    FailedContractSkeletonGenerationException,
    InvalidArgumentException,
    SodaCloudException,
)
from soda_core.common.soda_cloud import (
    ContractSkeletonGenerationState,
    SodaCloud,
    _build_check_collection_results_json_dict,
    _build_diagnostics_json_dict,
    _build_token_usage_dicts,
    _build_v4_diagnostics_check_type_json_dict,
)
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource
from soda_core.contracts.contract_publication import ContractPublicationResult
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    ContractVerificationResult,
    ContractVerificationSession,
    ContractVerificationSessionResult,
    ContractVerificationStatus,
    PostProcessingStage,
    PostProcessingStageState,
    ScanTokenUsage,
)
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import (
    ContractImpl,
    ContractVerificationHandler,
    ContractVerificationHandlerRegistry,
    ContractVerificationSessionImpl,
)
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.contracts.impl.diagnostics_warehouse_files import DiagnosticsWarehouseFiles

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


def test_soda_cloud_results_with_additional_threshold(data_source_test_helper: DataSourceTestHelper, env_vars: dict):
    """End-to-end through the engine exit: a threshold + `additional:` pair must reach the
    Cloud payload as BOTH `diagnostics.fail` and `diagnostics.warn`. The impl-level tests
    assert `CheckImpl.warn_threshold` and the wire tests hand-build the `Check` dataclass,
    so without this test the `warn_threshold=` kwarg in `_build_check_info` — the only
    carrier of the warn threshold out of the engine — could be dropped with the whole
    suite staying green."""
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
                    ],
                },
            ),
        ]
    )

    data_source_test_helper.assert_contract_warn(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
              - name: age
                missing_values: [-1, -2]
                checks:
                  - missing:
                      threshold:
                        must_be_less_than_or_equal: 5
                        additional:
                          must_be_less_than_or_equal: 1
                          level: warn
        """,
    )

    results_request: MockRequest = data_source_test_helper.soda_cloud.requests[1]
    assert results_request.json["type"] == "sodaCoreInsertScanResults"
    assert_dict(
        results_request.json,
        {
            "checks": [
                {
                    "checkPath": "columns.age.checks.missing",
                    "outcome": "warn",
                    "diagnostics": {
                        "value": 2,
                        "fail": {"greaterThan": 5},
                        "warn": {"greaterThan": 1},
                    },
                },
            ],
        },
    )


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

    # Without check paths and check filters the command carries no executionOptions.
    assert _runner_commands(data_source_test_helper.soda_cloud) == [
        {
            "type": "sodaCoreVerifyContract",
            "contract": {"fileId": "fffileid", "metadata": {"source": {"type": "local", "filePath": "REMOTE"}}},
            "verbose": False,
            "variables": {},
        }
    ]


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
    assert res.status is ContractVerificationStatus.ERROR
    assert res.has_errors
    assert any("insufficient permissions" in error for error in res.get_errors())


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
    assert res.status is ContractVerificationStatus.ERROR
    assert res.has_errors
    assert any("insufficient permissions" in error for error in res.get_errors())


@mock.patch("requests.post")
def test_fetch_contract(mock_post):
    soda_cloud = SodaCloud.from_yaml_source(YAML_SOURCE, provided_variable_values={})
    soda_cloud.token = "some_token"
    mock_post.return_value = MockResponse(status_code=200, json_object={"results": [{"contents": "contract_contents"}]})

    soda_cloud.fetch_contract(dataset_identifier=DatasetIdentifier.parse("test/some/schema/CUSTOMERS"))
    mock_post.assert_called_once_with(
        url="https://dev.sodadata.io/api/query",
        headers={"User-Agent": f"soda-core/{SODA_CORE_VERSION}"},
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
        headers={"User-Agent": f"soda-core/{SODA_CORE_VERSION}"},
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
        headers={"User-Agent": f"soda-core/{SODA_CORE_VERSION}"},
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
        headers={"User-Agent": f"soda-core/{SODA_CORE_VERSION}"},
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


def _build_freshness_check_result(unit: str, threshold_value: float) -> "FreshnessCheckResult":
    from soda_core.contracts.impl.check_types.freshness_check import FreshnessCheckResult

    return FreshnessCheckResult(
        check=mock.MagicMock(),
        outcome=CheckOutcome.PASSED,
        threshold_value=threshold_value,
        diagnostic_metric_values={
            "dataset_rows_tested": 6,
            "check_rows_tested": 6,
            f"freshness_in_{unit}s": threshold_value,
        },
        max_timestamp=datetime(2025, 1, 4, 9, 0, 0, tzinfo=timezone.utc),
        max_timestamp_utc=datetime(2025, 1, 4, 9, 0, 0, tzinfo=timezone.utc),
        data_timestamp=datetime(2025, 1, 4, 10, 0, 0, tzinfo=timezone.utc),
        data_timestamp_utc=datetime(2025, 1, 4, 10, 0, 0, tzinfo=timezone.utc),
        freshness="1:00:00",
        freshness_in_seconds=3600,
        unit=unit,
    )


def test_build_diagnostics_json_dict_sets_measure_time_for_freshness():
    # V4 (contract-scan) freshness values rendered as raw floats in the check charts
    # because soda-core never emitted a unit/type marker. Soda Cloud reads
    # ``diagnostics.measure`` (BE ``GenericCoreCheckDiagnostics.getMeasure()``);
    # for freshness it must be ``"time"`` so the value is formatted as a duration.
    check_result = _build_freshness_check_result(unit="hour", threshold_value=1.0)

    diagnostics = _build_diagnostics_json_dict(check_result)

    assert diagnostics["measure"] == "time"
    # V3 wire contract (soda-library FreshnessCheck): the "time" measure value is in
    # milliseconds, scaled from the check's configured `unit`. 1.0 hour -> 3_600_000 ms.
    assert diagnostics["value"] == 3_600_000


def test_build_diagnostics_json_dict_scales_freshness_days_to_milliseconds():
    # Locks the unit -> milliseconds scale table for the largest unit. 1.0 day ->
    # 86_400_000 ms. This is the float the ticket showed rendered raw ("11.15...").
    check_result = _build_freshness_check_result(unit="day", threshold_value=1.0)

    diagnostics = _build_diagnostics_json_dict(check_result)

    assert diagnostics["measure"] == "time"
    assert diagnostics["value"] == 86_400_000


def test_build_diagnostics_json_dict_omits_measure_for_non_time_checks():
    # Non-duration checks (e.g. row_count) carry no measure marker, so the key is
    # absent and Soda Cloud keeps formatting the value as a plain number. This keeps
    # the existing payload byte-for-byte unchanged for all non-time check types.
    check_result = CheckResult(
        check=mock.MagicMock(),
        outcome=CheckOutcome.PASSED,
        threshold_value=42,
    )

    diagnostics = _build_diagnostics_json_dict(check_result)

    assert "measure" not in diagnostics


# Every check type whose v4 diagnostics carry the dataset rows tested, with the diagnostics its evaluate writes.
V4_ROWS_TESTED_DIAGNOSTICS: dict[str, dict] = {
    "missing": {"missing_count": 1, "missing_percent": 50.0, "check_rows_tested": 2, "dataset_rows_tested": 5},
    "invalid": {
        "invalid_count": 1,
        "invalid_percent": 50.0,
        "check_rows_tested": 2,
        "missing_count": 0,
        "dataset_rows_tested": 5,
    },
    "duplicate": {
        "duplicate_count": 1,
        "duplicate_percent": 50.0,
        "check_rows_tested": 2,
        "missing_count": 0,
        "dataset_rows_tested": 5,
    },
    "failed_rows": {
        "failed_rows_count": 1,
        "failed_rows_percent": 50.0,
        "check_rows_tested": 2,
        "dataset_rows_tested": 5,
    },
    "aggregate": {"avg": 1.5, "check_rows_tested": 2, "dataset_rows_tested": 5},
    "metric": {"dataset_rows_tested": 5},
    "row_count": {"check_rows_tested": 2, "dataset_rows_tested": 5},
    "freshness": {"dataset_rows_tested": 5, "check_rows_tested": 6, "freshness_in_hours": 1.0},
}


def _v4_rows_tested_check_result(check_type: str, diagnostic_metric_values: dict) -> CheckResult:
    if check_type == "freshness":
        check_result = _build_freshness_check_result(unit="hour", threshold_value=1.0)
        check_result.diagnostic_metric_values = diagnostic_metric_values
        return check_result
    check = mock.MagicMock()
    check.type = check_type
    return CheckResult(
        check=check, outcome=CheckOutcome.PASSED, threshold_value=1, diagnostic_metric_values=diagnostic_metric_values
    )


@pytest.mark.parametrize("check_type", list(V4_ROWS_TESTED_DIAGNOSTICS))
def test_v4_diagnostics_carry_scope_rows_tested_only_when_present(check_type: str):
    unscoped_values = dict(V4_ROWS_TESTED_DIAGNOSTICS[check_type])
    unscoped = _build_v4_diagnostics_check_type_json_dict(_v4_rows_tested_check_result(check_type, unscoped_values))
    assert unscoped["datasetRowsTested"] == 5
    assert "scopeRowsTested" not in unscoped

    scoped = _build_v4_diagnostics_check_type_json_dict(
        _v4_rows_tested_check_result(check_type, {**unscoped_values, "scope_rows_tested": 2})
    )
    assert scoped == {**unscoped, "scopeRowsTested": 2}

    # An unmeasured scope count keeps the key; the serializer drops None values later.
    unmeasured = _build_v4_diagnostics_check_type_json_dict(
        _v4_rows_tested_check_result(check_type, {**unscoped_values, "scope_rows_tested": None})
    )
    assert unmeasured == {**unscoped, "scopeRowsTested": None}


def test_autogenerated_v4_diagnostics_camel_case_scope_rows_tested():
    check = mock.MagicMock()
    check.type = "some_extension_check"
    check_result = CheckResult(
        check=check,
        outcome=CheckOutcome.PASSED,
        diagnostic_metric_values={"dataset_rows_tested": 5, "scope_rows_tested": 2},
        autogenerate_diagnostics_payload=True,
    )
    assert _build_v4_diagnostics_check_type_json_dict(check_result) == {
        "type": "someExtensionCheck",
        "datasetRowsTested": 5,
        "scopeRowsTested": 2,
    }


def test_build_token_usage_dicts_serialization():
    mock_result = mock.MagicMock()
    mock_result.token_usage = [
        ScanTokenUsage(
            prompt_tokens=1500,
            completion_tokens=500,
            total_tokens=2000,
            model="gpt-4o",
            operation="autopilot",
            agent_source="SODA",
        ),
        ScanTokenUsage(
            prompt_tokens=300,
            completion_tokens=100,
            total_tokens=400,
            model="gpt-4o-mini",
            operation="autopilot",
        ),
    ]
    token_dicts = _build_token_usage_dicts(mock_result)
    assert token_dicts == [
        {
            "promptTokens": 1500,
            "completionTokens": 500,
            "totalTokens": 2000,
            "model": "gpt-4o",
            "operation": "autopilot",
            "agentSource": "SODA",
        },
        {
            "promptTokens": 300,
            "completionTokens": 100,
            "totalTokens": 400,
            "model": "gpt-4o-mini",
            "operation": "autopilot",
        },
    ]


def test_build_token_usage_dicts_empty_when_no_usage():
    mock_result = mock.MagicMock()
    mock_result.token_usage = None
    assert _build_token_usage_dicts(mock_result) == []

    mock_result.token_usage = []
    assert _build_token_usage_dicts(mock_result) == []


def _build_result_with_token_usage(token_usage: Optional[list[ScanTokenUsage]]) -> mock.MagicMock:
    timestamp = datetime(2026, 8, 18, tzinfo=timezone.utc)
    result = mock.MagicMock()
    result.check_collection = mock.MagicMock(soda_qualified_dataset_name="test_ds/schema/table")
    result.data_source = None
    result.data_timestamp = timestamp
    result.started_timestamp = timestamp
    result.ended_timestamp = timestamp
    result.check_results = []
    result.log_records = None
    result.post_processing_stages = None
    result.token_usage = token_usage
    result.measurement_dicts = []
    result.has_errors = False
    result.is_warned = False
    result.is_failed = False
    return result


def test_build_check_collection_results_serializes_token_usage_in_result_and_entry_order():
    results = [
        _build_result_with_token_usage(None),
        _build_result_with_token_usage(
            [
                ScanTokenUsage(10, 20, 30, model="model-a", operation="autopilot", agent_source="SODA"),
                ScanTokenUsage(40, 50, 90, model="model-b", operation="llmCheck"),
            ]
        ),
        _build_result_with_token_usage([]),
        _build_result_with_token_usage(
            [ScanTokenUsage(60, 70, 130, model="model-c", operation="autopilot", agent_source="BYOK")]
        ),
    ]

    payload = _build_check_collection_results_json_dict(results, wire_source="data-standard")

    assert payload["tokenUsage"] == [
        {
            "promptTokens": 10,
            "completionTokens": 20,
            "totalTokens": 30,
            "model": "model-a",
            "operation": "autopilot",
            "agentSource": "SODA",
        },
        {
            "promptTokens": 40,
            "completionTokens": 50,
            "totalTokens": 90,
            "model": "model-b",
            "operation": "llmCheck",
        },
        {
            "promptTokens": 60,
            "completionTokens": 70,
            "totalTokens": 130,
            "model": "model-c",
            "operation": "autopilot",
            "agentSource": "BYOK",
        },
    ]


def test_build_check_collection_results_keeps_empty_token_usage_list():
    payload = _build_check_collection_results_json_dict(
        [_build_result_with_token_usage(None), _build_result_with_token_usage([])],
        wire_source="data-standard",
    )

    assert payload["tokenUsage"] == []


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
        headers={"User-Agent": f"soda-core/{SODA_CORE_VERSION}"},
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


# ---- verify_contract_on_runner: every path without an outcome from Cloud must be ERROR ----

_RUNNER_CONTRACT_YAML = """
dataset: test/some/schema/CUSTOMERS
columns:
- name: id
"""


def _runner_allowed() -> MockResponse:
    return MockResponse(status_code=200, json_object={"allowed": True})


def _runner_uploaded() -> MockResponse:
    return MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": "fffileid"})


def _runner_scan_created() -> MockResponse:
    return MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"scanId": "ssscanid"})


def _runner_scan_state(state: str) -> MockResponse:
    return MockResponse(
        method=MockHttpMethod.GET,
        status_code=200,
        json_object={
            "scanId": "ssscanid",
            "state": state,
            "contractDatasetCloudUrl": "https://the-contract-dataset-url",
        },
    )


def _runner_scan_logs() -> MockResponse:
    return MockResponse(
        method=MockHttpMethod.GET,
        status_code=200,
        json_object={
            "content": [],
            "totalElements": 0,
            "totalPages": 1,
            "number": 0,
            "size": 0,
            "last": True,
            "first": True,
        },
    )


def _verify_on_runner_with(cloud: MockSodaCloud, blocking_timeout_in_minutes: int = 60) -> ContractVerificationResult:
    return cloud.verify_contract_on_runner(
        ContractYaml.parse(ContractYamlSource.from_str(_RUNNER_CONTRACT_YAML)),
        variables={},
        blocking_timeout_in_minutes=blocking_timeout_in_minutes,
        publish_results=False,
        verbose=False,
    )


def _verify_on_runner(
    responses: list[MockResponse], blocking_timeout_in_minutes: int = 60
) -> ContractVerificationResult:
    return _verify_on_runner_with(MockSodaCloud(responses), blocking_timeout_in_minutes)


def _exit_code(result: ContractVerificationResult) -> ExitCode:
    return interpret_contract_verification_result(ContractVerificationSessionResult([result]))


def test_verify_contract_on_runner_permission_query_failed():
    res = _verify_on_runner([MockResponse(status_code=500, json_object={})])

    assert res.status is ContractVerificationStatus.ERROR
    assert res.has_errors
    assert res.sending_results_to_soda_cloud_failed is True
    assert _exit_code(res) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_verify_contract_on_runner_upload_failed_returns_error_result():
    res = _verify_on_runner(
        [_runner_allowed(), MockResponse(method=MockHttpMethod.POST, status_code=500, json_object={})]
    )

    assert isinstance(res, ContractVerificationResult)
    assert res.status is ContractVerificationStatus.ERROR
    assert res.sending_results_to_soda_cloud_failed is True
    assert _exit_code(res) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_verify_contract_on_runner_verify_command_failed():
    res = _verify_on_runner(
        [
            _runner_allowed(),
            _runner_uploaded(),
            MockResponse(method=MockHttpMethod.POST, status_code=500, json_object={}),
        ]
    )

    assert res.status is ContractVerificationStatus.ERROR
    assert res.sending_results_to_soda_cloud_failed is True
    assert _exit_code(res) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_verify_contract_on_runner_without_scan_id():
    res = _verify_on_runner(
        [
            _runner_allowed(),
            _runner_uploaded(),
            MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={}),
        ]
    )

    assert res.status is ContractVerificationStatus.ERROR
    assert res.has_errors
    assert res.sending_results_to_soda_cloud_failed is False
    assert any("Scan ID" in error for error in res.get_errors())
    assert _exit_code(res) == ExitCode.LOG_ERRORS


def test_verify_contract_on_runner_poll_timeout():
    res = _verify_on_runner(
        [_runner_allowed(), _runner_uploaded(), _runner_scan_created(), _runner_scan_logs()],
        blocking_timeout_in_minutes=0,
    )

    assert res.status is ContractVerificationStatus.ERROR
    assert res.sending_results_to_soda_cloud_failed is True
    assert any("did not finish" in error for error in res.get_errors())
    assert _exit_code(res) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@pytest.mark.parametrize(
    "state, expected_status, expected_exit_code",
    [
        ("completed", ContractVerificationStatus.PASSED, ExitCode.OK),
        ("completedWithWarnings", ContractVerificationStatus.WARNED, ExitCode.CHECK_WARNINGS),
        ("completedWithFailures", ContractVerificationStatus.FAILED, ExitCode.CHECK_FAILURES),
        ("completedWithErrors", ContractVerificationStatus.ERROR, ExitCode.LOG_ERRORS),
        ("failed", ContractVerificationStatus.ERROR, ExitCode.LOG_ERRORS),
        ("canceled", ContractVerificationStatus.ERROR, ExitCode.LOG_ERRORS),
        ("timedOut", ContractVerificationStatus.ERROR, ExitCode.LOG_ERRORS),
    ],
)
def test_verify_contract_on_runner_final_state(state, expected_status, expected_exit_code):
    res = _verify_on_runner(
        [
            _runner_allowed(),
            _runner_uploaded(),
            _runner_scan_created(),
            _runner_scan_state(state),
            _runner_scan_logs(),
        ]
    )

    assert res.status is expected_status
    assert res.scan_id == "ssscanid"
    assert res.sending_results_to_soda_cloud_failed is False
    assert _exit_code(res) == expected_exit_code
    if expected_status is ContractVerificationStatus.ERROR:
        assert any(f"'{state}'" in error for error in res.get_errors())


def test_execute_on_runner_isolates_exceptions_as_error_results(monkeypatch):
    mock_cloud = MockSodaCloud([])

    def raise_boom(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(mock_cloud, "verify_contract_on_runner", raise_boom)

    results = ContractVerificationSessionImpl._execute_on_runner(
        contract_yaml_sources=[ContractYamlSource.from_str(_RUNNER_CONTRACT_YAML)],
        variables={},
        soda_cloud_impl=mock_cloud,
        soda_cloud_use_runner_blocking_timeout_in_minutes=60,
        soda_cloud_publish_results=False,
        soda_cloud_verbose=False,
    )

    assert len(results) == 1
    assert results[0].status is ContractVerificationStatus.ERROR
    assert isinstance(results[0].error, RuntimeError)
    assert any("boom" in error for error in results[0].get_errors())
    assert _exit_code(results[0]) == ExitCode.LOG_ERRORS


def test_verify_contract_on_runner_permission_query_unreachable(monkeypatch):
    mock_cloud = MockSodaCloud([])

    def raise_connection_error(*args, **kwargs):
        raise ConnectionError("cloud down")

    monkeypatch.setattr(mock_cloud, "_http_post", raise_connection_error)

    res = _verify_on_runner_with(mock_cloud)

    assert res.status is ContractVerificationStatus.ERROR
    assert res.sending_results_to_soda_cloud_failed is True
    assert _exit_code(res) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_verify_contract_on_runner_keeps_result_when_the_path_raises(monkeypatch):
    mock_cloud = MockSodaCloud([_runner_allowed(), _runner_uploaded(), _runner_scan_created()])

    def raise_boom(**kwargs):
        raise RuntimeError("boom while polling")

    monkeypatch.setattr(mock_cloud, "_poll_remote_scan_finished", raise_boom)

    res = _verify_on_runner_with(mock_cloud)

    assert res.status is ContractVerificationStatus.ERROR
    assert res.check_collection.dataset_name == "CUSTOMERS"
    assert res.scan_id == "ssscanid"
    assert any("boom while polling" in error for error in res.get_errors())
    assert _exit_code(res) == ExitCode.LOG_ERRORS


def test_verify_contract_on_runner_survives_log_fetch_failure(monkeypatch):
    mock_cloud = MockSodaCloud(
        [_runner_allowed(), _runner_uploaded(), _runner_scan_created(), _runner_scan_state("completed")]
    )

    def raise_connection_error(**kwargs):
        raise ConnectionError("logs endpoint down")

    monkeypatch.setattr(mock_cloud, "_get_scan_logs", raise_connection_error)

    res = _verify_on_runner_with(mock_cloud)

    assert res.status is ContractVerificationStatus.PASSED
    assert not res.has_errors
    assert any("logs endpoint down" in warning for warning in res.get_warnings())
    assert _exit_code(res) == ExitCode.OK


def test_verify_contract_on_runner_keeps_polling_through_unknown_state(monkeypatch):
    monkeypatch.setattr("soda_core.common.soda_cloud.sleep", lambda seconds: None)

    res = _verify_on_runner(
        [
            _runner_allowed(),
            _runner_uploaded(),
            _runner_scan_created(),
            _runner_scan_state("started"),
            _runner_scan_state("completed"),
            _runner_scan_logs(),
        ]
    )

    assert res.status is ContractVerificationStatus.PASSED
    assert any("unknown state 'started'" in warning for warning in res.get_warnings())


def test_verify_contract_on_runner_keeps_polling_through_status_request_failure(monkeypatch):
    monkeypatch.setattr("soda_core.common.soda_cloud.sleep", lambda seconds: None)
    mock_cloud = MockSodaCloud(
        [
            _runner_allowed(),
            _runner_uploaded(),
            _runner_scan_created(),
            _runner_scan_state("completed"),
            _runner_scan_logs(),
        ]
    )
    original_get_scan_status = mock_cloud._get_scan_status
    calls = {"count": 0}

    def flaky_get_scan_status(scan_id):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ConnectionError("connection reset by peer")
        return original_get_scan_status(scan_id)

    monkeypatch.setattr(mock_cloud, "_get_scan_status", flaky_get_scan_status)

    res = _verify_on_runner_with(mock_cloud)

    assert res.status is ContractVerificationStatus.PASSED
    assert calls["count"] == 2
    assert any("retrying" in warning for warning in res.get_warnings())


# ---- verify on the runner: check paths and check filters go up as the command's executionOptions ----

_SCOPED_RUNNER_CONTRACT_YAML = """
dataset: test/some/schema/CUSTOMERS
scopes:
  eu:
    name: EU
  us:
    name: US
  apac:
    name: APAC
columns:
- name: id
checks:
- row_count:
- row_count:
    scope: eu
"""

_EU_US_RUNNER_CONTRACT_YAML = """
dataset: test/some/schema/CUSTOMERS
scopes:
  eu:
    name: EU
  us:
    name: US
columns:
- name: id
"""

_RUNNER_COMMAND_TYPES = ("sodaCoreVerifyContract", "sodaCoreTestContract")


def _runner_commands(cloud: MockSodaCloud) -> list[dict]:
    """The runner commands Cloud received, without the token the client adds to every request."""
    return [
        {key: value for key, value in request.json.items() if key != "token"}
        for request in cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") in _RUNNER_COMMAND_TYPES
    ]


def _runner_completed() -> list[MockResponse]:
    return [
        _runner_allowed(),
        _runner_uploaded(),
        _runner_scan_created(),
        _runner_scan_state("completed"),
        _runner_scan_logs(),
    ]


def _runner_command_rejected(code: str, message: str) -> list[MockResponse]:
    return [
        _runner_allowed(),
        _runner_uploaded(),
        MockResponse(method=MockHttpMethod.POST, status_code=400, json_object={"code": code, "message": message}),
    ]


def _execute_session_on_runner(
    cloud: MockSodaCloud,
    check_paths: Optional[list[str]] = None,
    check_filters: Optional[list[str]] = None,
    publish: bool = True,
    contract_yaml: str = _SCOPED_RUNNER_CONTRACT_YAML,
) -> ContractVerificationSessionResult:
    return ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract_yaml)],
        soda_cloud_impl=cloud,
        soda_cloud_publish_results=publish,
        soda_cloud_use_runner=True,
        check_paths=check_paths,
        check_selectors=CheckSelector.parse_all(check_filters),
    )


@pytest.mark.parametrize("publish, command_type", [(True, "sodaCoreVerifyContract"), (False, "sodaCoreTestContract")])
def test_runner_command_carries_check_paths_and_check_filters(publish: bool, command_type: str):
    cloud = MockSodaCloud(_runner_completed())

    session_result = _execute_session_on_runner(
        cloud,
        check_paths=["a", "b"],
        check_filters=["scope=eu", "scope=us", "scope!=apac"],
        publish=publish,
    )

    assert interpret_contract_verification_result(session_result) == ExitCode.OK
    [command] = _runner_commands(cloud)
    assert command["type"] == command_type
    # The check paths go up as given, and never as a check_path filter.
    assert command["executionOptions"] == {
        "checkPaths": ["a", "b"],
        "checkFilters": [
            {"field": "scope", "values": ["eu", "us"], "negate": False},
            {"field": "scope", "values": ["apac"], "negate": True},
        ],
    }
    assert list(command) == ["type", "contract", "verbose", "variables", "executionOptions"]


def test_runner_command_carries_a_negated_check_filter():
    cloud = MockSodaCloud(_runner_completed())

    _execute_session_on_runner(cloud, check_filters=["scope!=eu"])

    [command] = _runner_commands(cloud)
    assert command["executionOptions"] == {"checkFilters": [{"field": "scope", "values": ["eu"], "negate": True}]}
    assert command["executionOptions"]["checkFilters"][0]["negate"] is True


def test_runner_command_without_check_paths_and_check_filters_is_unchanged():
    cloud = MockSodaCloud(_runner_completed())

    _execute_session_on_runner(cloud)

    [command] = _runner_commands(cloud)
    assert list(command) == ["type", "contract", "verbose", "variables"]
    assert command == {
        "type": "sodaCoreVerifyContract",
        "contract": {"fileId": "fffileid", "metadata": {"source": {"type": "local", "filePath": "REMOTE"}}},
        "verbose": False,
        "variables": {},
    }


@pytest.mark.parametrize(
    "check_paths, check_filters, expected_execution_options",
    [
        pytest.param(
            [], ["scope=eu"], {"checkFilters": [{"field": "scope", "values": ["eu"], "negate": False}]}, id="no-paths"
        ),
        pytest.param(["a"], None, {"checkPaths": ["a"]}, id="no-filters"),
        pytest.param([], [], None, id="neither"),
    ],
)
def test_runner_command_omits_an_empty_list(
    check_paths: list[str], check_filters: Optional[list[str]], expected_execution_options: Optional[dict]
):
    cloud = MockSodaCloud(_runner_completed())

    _execute_session_on_runner(cloud, check_paths=check_paths, check_filters=check_filters)

    [command] = _runner_commands(cloud)
    assert ("executionOptions" in command) is (expected_execution_options is not None)
    assert command.get("executionOptions") == expected_execution_options


def test_runner_command_has_one_check_filter_per_field_and_polarity():
    cloud = MockSodaCloud(_runner_completed())

    _execute_session_on_runner(
        cloud,
        check_filters=["name=row_count", "scope!=eu", "attributes.team=a", "scope=us", "name=missing", "scope=us"],
    )

    # In the order of each field and polarity's first filter, values in the order given, a repeated value once.
    # Fields other than scope go up as written, for Cloud to reject.
    [command] = _runner_commands(cloud)
    assert command["executionOptions"] == {
        "checkFilters": [
            {"field": "name", "values": ["row_count", "missing"], "negate": False},
            {"field": "scope", "values": ["eu"], "negate": True},
            {"field": "attributes.team", "values": ["a"], "negate": False},
            {"field": "scope", "values": ["us"], "negate": False},
        ]
    }


def test_runner_command_sends_base_and_wildcard_scope_values_without_a_local_check():
    cloud = MockSodaCloud(_runner_completed())

    session_result = _execute_session_on_runner(
        cloud, check_filters=["scope=base", "scope=a*", "scope!=?u"], contract_yaml=_EU_US_RUNNER_CONTRACT_YAML
    )

    assert interpret_contract_verification_result(session_result) == ExitCode.OK
    [command] = _runner_commands(cloud)
    assert command["executionOptions"] == {
        "checkFilters": [
            {"field": "scope", "values": ["base", "a*"], "negate": False},
            {"field": "scope", "values": ["?u"], "negate": True},
        ]
    }


def test_runner_command_rejected_by_cloud_ends_in_error_with_the_cloud_message():
    cloud = MockSodaCloud(
        _runner_command_rejected("invalid_contract", "checkFilters field is missing or not one of: scope")
    )

    session_result = _execute_session_on_runner(cloud, check_filters=["name=row_count"])

    [command] = _runner_commands(cloud)
    assert command["executionOptions"] == {
        "checkFilters": [{"field": "name", "values": ["row_count"], "negate": False}]
    }
    [result] = session_result.contract_verification_results
    assert result.status is ContractVerificationStatus.ERROR
    assert result.sending_results_to_soda_cloud_failed is True
    assert any(
        "checkFilters field is missing or not one of: scope" in error and "invalid_contract" in error
        for error in result.get_errors()
    )
    assert interpret_contract_verification_result(session_result) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_runner_sends_a_contradiction_up_unchanged():
    message = "checkFilters value eu of field scope must not be both included and excluded"
    cloud = MockSodaCloud(_runner_command_rejected("invalid_contract", message))

    session_result = _execute_session_on_runner(cloud, check_filters=["scope=eu", "scope!=eu"])

    [command] = _runner_commands(cloud)
    assert command["executionOptions"] == {
        "checkFilters": [
            {"field": "scope", "values": ["eu"], "negate": False},
            {"field": "scope", "values": ["eu"], "negate": True},
        ]
    }
    [result] = session_result.contract_verification_results
    assert result.status is ContractVerificationStatus.ERROR
    assert any(message in error for error in result.get_errors())
    assert interpret_contract_verification_result(session_result) == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@pytest.mark.parametrize(
    "check_filter, unknown_key",
    [("scope=apac", "apac"), ("scope!=apac", "apac"), ("scope=[eu,us]", "[eu,us]")],
)
def test_runner_refuses_an_unknown_scope_key_before_any_cloud_request(check_filter: str, unknown_key: str):
    cloud = MockSodaCloud([])

    with pytest.raises(InvalidArgumentException, match=re.escape(f"'{unknown_key}'")):
        _execute_session_on_runner(
            cloud, check_filters=["scope=eu", check_filter], contract_yaml=_EU_US_RUNNER_CONTRACT_YAML
        )

    assert cloud.requests == []
