import pytest
from helpers.mock_soda_cloud import MockHttpMethod, MockResponse, MockSodaCloud
from soda_core.common.exceptions import YamlParserException
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.contracts.contract_publication import (
    ContractPublication,
    ContractPublicationResult,
    ContractPublicationResultList,
)


def test_contract_publication_fails_on_missing_soda_cloud_config():
    contract_publication_result: ContractPublicationResult = (
        ContractPublication.builder()
        .with_contract_yaml_str(
            f"""
          dataset: ds/db/sch/CUSTOMERS
          columns:
            - name: id
        """
        )
        .build()
        .execute()
    )

    assert contract_publication_result.has_errors
    assert "Cannot publish without a Soda Cloud configuration" in contract_publication_result.logs.get_logs_str()
    assert (
        "skipping publication because of missing Soda Cloud configuration"
        in contract_publication_result.logs.get_logs_str()
    )


def test_contract_publication_fails_on_missing_contract_file():
    responses = [
        MockResponse(
            status_code=200,
            json_object={
                "allowed": True,
            },
        ),
    ]
    mock_cloud = MockSodaCloud(responses)
    with pytest.raises(YamlParserException):
        contract_publication_result: ContractPublicationResult = (
            ContractPublication.builder()
            .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
            .with_soda_cloud_yaml_str(
                """
            soda_cloud:
              host: host.soda.io
              api_key_id: id
              api_key_secret: secret
            """
            )
            .with_soda_cloud(mock_cloud)
            .build()
            .execute()
        )

        assert contract_publication_result.has_errors
        assert (
            "Contract file '../soda/mydb/myschema/table.yml' does not exist"
            in contract_publication_result.logs.get_errors_str()
        )


def test_contract_publication_returns_result_for_each_added_contract():
    responses = [
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"allowed": "true"}),
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": "fake_file_id"}),
        MockResponse(
            method=MockHttpMethod.POST,
            json_object={
                "publishedContract": {
                    "checksum": "check",
                    "fileId": "fake_file_id",
                },
                "metadata": {"source": {"filePath": "contract1.yml", "type": "local"}},
            },
        ),
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"allowed": "true"}),
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": "fake_file_id2"}),
        MockResponse(
            method=MockHttpMethod.POST,
            json_object={
                "publishedContract": {
                    "checksum": "check",
                    "fileId": "fake_file_id2",
                },
                "metadata": {"source": {"filePath": "contract2.yml", "type": "local"}},
            },
        ),
    ]
    mock_cloud = MockSodaCloud(responses)

    contract_publication_result = (
        ContractPublication.builder()
        .with_contract_yaml_str(
            f"""
            dataset: test/some/schema/CUSTOMERS
            columns:
            - name: id
            """
        )
        .with_contract_yaml_str(
            f"""
            dataset: test2/some/schema/CUSTOMERS2
            columns:
            - name: id
            """
        )
        .with_soda_cloud(mock_cloud)
        .build()
        .execute()
    )

    assert isinstance(contract_publication_result, ContractPublicationResultList)
    assert len(contract_publication_result) == 2
    assert not contract_publication_result.has_errors

    assert contract_publication_result[0].contract.data_source_name == "test"
    assert contract_publication_result[1].contract.data_source_name == "test2"


VALID_CONTRACT_YAML = """dataset: ds/db/sch/CUSTOMERS
columns:
  - name: id
"""

# Parses as YAML, but 'filter' must be a string.
YAML_ERROR_CONTRACT_YAML = """dataset: ds/db/sch/CUSTOMERS
filter: [id, name]
columns:
  - name: id
"""

DUPLICATE_COLUMNS_CONTRACT_YAML = """dataset: ds/db/sch/CUSTOMERS
columns:
  - name: id
  - name: id
"""

# Publish needs no variable values. A variable without a default gets its value when the contract
# is verified, and publish uploads the contract text with the variable reference unresolved.
REQUIRED_VARIABLE_CONTRACT_YAML = """dataset: ds/db/sch/CUSTOMERS
variables:
  START_DATE:
filter: "created_at >= '${var.START_DATE}'"
columns:
  - name: id
"""

METRIC_QUERY_VARIABLE_CONTRACT_YAML = """dataset: ds/db/sch/CUSTOMERS
variables:
  QUERY:
columns:
  - name: id
checks:
  - metric:
      query: ${var.QUERY}
      threshold:
        must_be_greater_than: 0
"""

FAILED_ROWS_QUERY_VARIABLE_CONTRACT_YAML = """dataset: ds/db/sch/CUSTOMERS
variables:
  QUERY:
columns:
  - name: id
checks:
  - failed_rows:
      query: ${var.QUERY}
"""

FAILED_ROWS_EXPRESSION_VARIABLE_CONTRACT_YAML = """dataset: ds/db/sch/CUSTOMERS
variables:
  CONDITION:
columns:
  - name: id
checks:
  - failed_rows:
      expression: ${var.CONDITION}
"""


def publish_responses(file_id: str = "fake_file_id") -> list[MockResponse]:
    return [
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"allowed": True}),
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": file_id}),
        MockResponse(
            method=MockHttpMethod.POST,
            status_code=200,
            json_object={
                "publishedContract": {"checksum": "check", "fileId": file_id},
                "metadata": {"source": {"filePath": None, "type": "local"}},
            },
        ),
    ]


def publish_request_jsons(contract_yaml_str: str, file_id: str = "fake_file_id") -> list[dict]:
    return [
        {
            "type": "sodaCoreCanManageContracts",
            "dataset": {"datasource": "ds", "prefixes": ["db", "sch"], "name": "CUSTOMERS"},
            "token": "mock-token",
        },
        {"type": "sodaCoreUploadContractFile", "contents": contract_yaml_str, "token": "mock-token"},
        {
            "type": "sodaCorePublishContract",
            "contract": {"fileId": file_id, "metadata": {"source": {"type": "local"}}},
            "token": "mock-token",
        },
    ]


def publish_contract_yaml_strs(mock_cloud: MockSodaCloud, *contract_yaml_strs: str) -> ContractPublicationResultList:
    builder = ContractPublication.builder()
    for contract_yaml_str in contract_yaml_strs:
        builder.with_contract_yaml_str(contract_yaml_str)
    return builder.with_soda_cloud(mock_cloud).build().execute()


@pytest.mark.parametrize(
    "contract_yaml_str, expected_error",
    [
        pytest.param(
            YAML_ERROR_CONTRACT_YAML,
            "YAML key 'filter' expected one of ['str'], but was YAML list",
            id="yaml_error",
        ),
        pytest.param(
            DUPLICATE_COLUMNS_CONTRACT_YAML,
            "Duplicate columns with name 'id': At file locations: [2,4], [3,4]",
            id="contract_validation_error",
        ),
    ],
)
def test_contract_publication_uploads_nothing_when_the_contract_has_errors(contract_yaml_str, expected_error):
    mock_cloud = MockSodaCloud(publish_responses())

    result = publish_contract_yaml_strs(mock_cloud, contract_yaml_str)

    assert mock_cloud.requests == []
    assert result.has_errors
    assert len(result) == 1
    assert result[0].contract is None
    assert result.logs.get_errors() == [
        expected_error,
        f"Skipping publication of the contract because it has 1 error: {expected_error}",
    ]


def test_contract_publication_names_every_error_of_the_contract():
    mock_cloud = MockSodaCloud(publish_responses())

    result = publish_contract_yaml_strs(
        mock_cloud,
        """dataset: ds/db/sch/CUSTOMERS
filter: [id, name]
columns:
  - name: id
  - name: id
""",
    )

    assert mock_cloud.requests == []
    assert result.logs.get_errors()[-1] == (
        "Skipping publication of the contract because it has 2 errors: "
        "YAML key 'filter' expected one of ['str'], but was YAML list; "
        "Duplicate columns with name 'id': At file locations: [3,4], [4,4]"
    )


def test_contract_publication_uploads_a_valid_contract_unchanged():
    mock_cloud = MockSodaCloud(publish_responses())

    result = publish_contract_yaml_strs(mock_cloud, VALID_CONTRACT_YAML)

    assert [request.json for request in mock_cloud.requests] == publish_request_jsons(VALID_CONTRACT_YAML)
    assert not result.has_errors
    assert len(result) == 1
    assert result[0].contract.soda_qualified_dataset_name == "ds/db/sch/CUSTOMERS"


@pytest.mark.parametrize(
    "contract_yaml_str",
    [
        pytest.param(REQUIRED_VARIABLE_CONTRACT_YAML, id="filter"),
        pytest.param(METRIC_QUERY_VARIABLE_CONTRACT_YAML, id="metric_query"),
        pytest.param(FAILED_ROWS_QUERY_VARIABLE_CONTRACT_YAML, id="failed_rows_query"),
        pytest.param(FAILED_ROWS_EXPRESSION_VARIABLE_CONTRACT_YAML, id="failed_rows_expression"),
    ],
)
def test_contract_publication_uploads_a_contract_with_a_variable_without_value_unchanged(contract_yaml_str):
    mock_cloud = MockSodaCloud(publish_responses())

    result = publish_contract_yaml_strs(mock_cloud, contract_yaml_str)

    assert [request.json for request in mock_cloud.requests] == publish_request_jsons(contract_yaml_str)
    assert not result.has_errors
    assert result.logs.get_errors() == []
    assert result[0].contract is not None


def test_contract_publication_uploads_nothing_when_a_contract_with_a_variable_without_value_has_another_error():
    mock_cloud = MockSodaCloud(publish_responses())

    result = publish_contract_yaml_strs(mock_cloud, REQUIRED_VARIABLE_CONTRACT_YAML + "  - name: id\n")

    assert mock_cloud.requests == []
    assert result[0].contract is None
    assert result.logs.get_errors() == [
        "Duplicate columns with name 'id': At file locations: [5,4], [6,4]",
        "Skipping publication of the contract because it has 1 error: "
        "Duplicate columns with name 'id': At file locations: [5,4], [6,4]",
    ]


@pytest.mark.parametrize(
    "contract_yaml_str, expected_error",
    [
        pytest.param(
            "dataset: ds/db/sch/CUSTOMERS\ncolumns:\n  - name: id\nchecks:\n  - not_a_check:\n",
            "Invalid check type 'not_a_check'. Existing check types: ",
            id="invalid_check_type",
        ),
        pytest.param(
            "dataset: ds/db/sch/CUSTOMERS\ncolumns:\n  - name: id\nchecks:\n  - row_count:\n"
            "      threshold:\n        must_be_between: 5\n",
            "YAML key 'must_be_between' expected one of ['dict'], but was int",
            id="invalid_threshold",
        ),
        pytest.param(
            'dataset: ds/db/sch/CUSTOMERS\nfilter: "id > ${var.UNDECLARED}"\ncolumns:\n  - name: id\n',
            "Variable 'UNDECLARED' was used and not declared",
            id="undeclared_variable",
        ),
        pytest.param(
            'dataset: ds/db/sch/CUSTOMERS\nfilter: "ts > ${soda.UNKNOWN}"\ncolumns:\n  - name: id\n',
            "Variable 'UNKNOWN' was used and not available in the 'soda' namespace",
            id="unknown_soda_variable",
        ),
    ],
)
def test_contract_publication_uploads_nothing_for_check_and_variable_errors(contract_yaml_str, expected_error):
    mock_cloud = MockSodaCloud(publish_responses())

    result = publish_contract_yaml_strs(mock_cloud, contract_yaml_str)

    assert mock_cloud.requests == []
    assert result.has_errors
    assert result[0].contract is None
    errors = result.logs.get_errors()
    assert len(errors) == 2
    # The list of existing check types depends on the check types registered in this process.
    assert errors[0].startswith(expected_error)
    assert errors[1] == f"Skipping publication of the contract because it has 1 error: {errors[0]}"


def test_contract_publication_skips_only_the_contract_with_errors():
    mock_cloud = MockSodaCloud(publish_responses())

    result = publish_contract_yaml_strs(mock_cloud, DUPLICATE_COLUMNS_CONTRACT_YAML, VALID_CONTRACT_YAML)

    assert [request.json for request in mock_cloud.requests] == publish_request_jsons(VALID_CONTRACT_YAML)
    assert result.has_errors
    assert len(result) == 2
    assert result[0].contract is None
    assert result[1].contract.soda_qualified_dataset_name == "ds/db/sch/CUSTOMERS"


def test_contract_publication_keeps_its_errors_when_another_logs_is_active():
    mock_cloud = MockSodaCloud(publish_responses())
    caller_logs = Logs()
    builder = ContractPublication.builder(logs=caller_logs).with_contract_yaml_str(DUPLICATE_COLUMNS_CONTRACT_YAML)
    # Constructing a Logs makes it the active capture target, so the caller's Logs is no longer active at build.
    other_logs = Logs()

    result = builder.with_soda_cloud(mock_cloud).build().execute()

    assert mock_cloud.requests == []
    assert result.has_errors
    assert result.logs is caller_logs
    assert caller_logs.get_errors() == [
        "Duplicate columns with name 'id': At file locations: [2,4], [3,4]",
        "Skipping publication of the contract because it has 1 error: "
        "Duplicate columns with name 'id': At file locations: [2,4], [3,4]",
    ]
    assert other_logs.get_errors() == []
    soda_logger.error("logged after the publication")
    assert other_logs.get_errors() == ["logged after the publication"]


def test_contract_publication_raises_on_a_yaml_syntax_error():
    mock_cloud = MockSodaCloud(publish_responses())

    with pytest.raises(YamlParserException, match="YAML syntax error"):
        publish_contract_yaml_strs(mock_cloud, "dataset: ds/db/sch/CUSTOMERS\ncolumns: [\n")

    assert mock_cloud.requests == []


# TODO @Niels: To be evaluated if still needed refactored after rework
# @pytest.mark.parametrize(
#     "logs, has_errors",
#     [
#         (Logs([Log(level=CRITICAL, message="critical")]), True),
#         (Logs([Log(level=ERROR, message="error")]), True),
#         (Logs([Log(level=ERROR, message="error"), Log(level=CRITICAL, message="critical")]), True),
#         (Logs([Log(level=WARN, message="warn"), Log(level=INFO, message="info")]), False),
#     ],
# )
# def test_contract_publication_log_levels(logs, has_errors):
#     result = ContractPublicationResultList(logs=logs, items=[ContractPublicationResult(contract=Mock(), logs=logs)])
#     assert result.has_errors is has_errors
