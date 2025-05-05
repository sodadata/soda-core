import os
from datetime import datetime

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import (
    MockHttpMethod,
    MockRequest,
    MockResponse,
    MockSodaCloud,
)
from helpers.test_table import TestTableSpecification
from soda_core.common.datetime_conversions import convert_datetime_to_str
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource
from soda_core.contracts.contract_publication import ContractPublicationResult
from soda_core.contracts.contract_verification import ContractVerificationResult
from soda_core.contracts.impl.contract_yaml import ContractYaml

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("soda_cloud")
    .column_text("id")
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


def test_soda_cloud_from_yaml_source_with_api_key_auth():
    yaml_source = SodaCloudYamlSource.from_str(
        """
        soda_cloud:
          host: dev.sodadata.io
          api_key_id: some_key_id
          api_key_secret: some_key_secret
        """
    )
    try:
        soda_cloud = SodaCloud.from_yaml_source(yaml_source, provided_variable_values={})
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
            # MockResponse(
            #     status_code=200,
            #     json_object={
            #       "allowed": True,
            #     }
            # ),
            MockResponse(status_code=200, json_object={"fileId": "777ggg"})
        ]
    )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
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
                      threshold:
                        must_be_less_than_or_equal: 5
            checks:
              - schema:
        """,
    )

    request_index = 0

    # request_0: MockRequest = data_source_test_helper.soda_cloud.requests[request_index]
    # assert request_0.url.endswith("api/query")
    # assert request_0.json["type"] == "sodaCoreContractCanBePublished"
    # request_index += 1

    request_1: MockRequest = data_source_test_helper.soda_cloud.requests[request_index]
    assert request_1.url.endswith("api/scan/upload")
    request_index += 1

    request_2: MockRequest = data_source_test_helper.soda_cloud.requests[request_index]
    assert request_2.url.endswith("api/command")
    assert request_2.json["type"] == "sodaCoreInsertScanResults"
    assert request_2.json["scanId"] == "env_var_scan_id"

    check_paths: list[str] = [c["checkPath"].lower() for c in request_2.json["checks"]]
    assert "columns.age.checks.missing" in check_paths
    assert "columns.age.checks.missing.2" in check_paths
    assert "checks.schema" in check_paths


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
                headers={"X-Soda-Next-Poll-Time": convert_datetime_to_str(datetime.now())},
                json_object={
                    "scanId": "ssscanid",
                    "state": "running",
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
