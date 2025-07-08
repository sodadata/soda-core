import pytest
from helpers.mock_soda_cloud import MockHttpMethod, MockResponse, MockSodaCloud, SequentialResponseRequestHandler
from soda_core.common.exceptions import InvalidDatasetQualifiedNameException, YamlParserException
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

    assert contract_publication_result.has_errors()
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

        assert contract_publication_result.has_errors()
        assert (
            "Contract file '../soda/mydb/myschema/table.yml' does not exist"
            in contract_publication_result.logs.get_errors_str()
        )


def test_contract_publication_returns_result_for_each_added_contract():
    mock_cloud = MockSodaCloud(
        request_handlers=[
            SequentialResponseRequestHandler(
                responses=[
                    MockResponse(status_code=200, json_object={"allowed": "true"}),
                    MockResponse(status_code=200, json_object={"fileId": "fake_file_id"}),
                    MockResponse(
                        json_object={
                            "publishedContract": {
                                "checksum": "check",
                                "fileId": "fake_file_id",
                            },
                            "metadata": {"source": {"filePath": "contract1.yml", "type": "local"}},
                        },
                    ),
                    MockResponse(status_code=200, json_object={"allowed": "true"}),
                    MockResponse(status_code=200, json_object={"fileId": "fake_file_id2"}),
                    MockResponse(
                        json_object={
                            "publishedContract": {
                                "checksum": "check",
                                "fileId": "fake_file_id2",
                            },
                            "metadata": {"source": {"filePath": "contract2.yml", "type": "local"}},
                        },
                    ),
                ]
            )
        ]
    )

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
    assert not contract_publication_result.has_errors()

    assert contract_publication_result[0].contract.data_source_name == "test"
    assert contract_publication_result[1].contract.data_source_name == "test2"


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
#     assert result.has_errors() is has_errors
