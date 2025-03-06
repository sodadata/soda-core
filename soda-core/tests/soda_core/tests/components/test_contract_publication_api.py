from soda_core.contracts.contract_publication import ContractPublication, ContractPublicationResultList, \
    ContractPublicationResult
from soda_core.tests.helpers.mock_soda_cloud import MockResponse, MockHttpMethod, MockSodaCloud


def test_contract_publication_fails_on_missing_soda_cloud_config():
    contract_publication_result = (
        ContractPublication.builder()
        .with_contract_yaml_str(f"""
          dataset: CUSTOMERS
          columns:
            - name: id
        """)
        .build()
        .execute()
    )

    assert contract_publication_result.has_errors()
    assert ("cannot publish without a Soda Cloud configuration" in str(contract_publication_result.logs))
    assert("skipping publication because of missing Soda Cloud configuration" in str(contract_publication_result.logs))

def test_contract_publication_fails_on_missing_contract_file():
    contract_publication_result = (
        ContractPublication.builder()
        .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
        .with_soda_cloud_yaml_str("""
        soda_cloud:
          host: host.soda.io
          api_key_id: id
          api_key_secret: secret
        """)
        .build()
        .execute()
    )

    assert contract_publication_result.has_errors()
    assert ("Contract file '../soda/mydb/myschema/table.yml' does not exist" in str(contract_publication_result.logs))


def test_contract_publication_returns_result_for_each_added_contract():
    responses = [
        MockResponse(
            method=MockHttpMethod.POST,
            status_code=200,
            json_object={"fileId": "fake_file_id"}
        ),
        MockResponse(
            method=MockHttpMethod.POST,
            json_object={
                'publishedContract': {
                    'checksum': 'check',
                    'fileId': 'fake_file_id',
                },
                'metadata': {
                    'source': {
                        'filePath': 'contract1.yml',
                        'type': 'local'
                    }
                }
            }
        ),
        MockResponse(
            method=MockHttpMethod.POST,
            status_code=200,
            json_object={"fileId": "fake_file_id2"}
        ),
        MockResponse(
            method=MockHttpMethod.POST,
            json_object={
                'publishedContract': {
                    'checksum': 'check',
                    'fileId': 'fake_file_id2',
                },
                'metadata': {
                    'source': {
                        'filePath': 'contract2.yml',
                        'type': 'local'
                    }
                }
            }
        )
    ]
    mock_cloud = MockSodaCloud(responses)

    contract_publication_result = (
        ContractPublication.builder()
        .with_contract_yaml_str(f"""
            dataset: CUSTOMERS
            dataset_prefix: [some, schema]
            data_source: test
            columns:
            - name: id
            """)
        .with_contract_yaml_str(f"""
            dataset: CUSTOMERS2
            dataset_prefix: [some, schema]
            data_source: test2
            columns:
            - name: id
            """)
        .with_soda_cloud(mock_cloud)
        .build()
        .execute()
    )

    assert isinstance(contract_publication_result, ContractPublicationResultList)
    assert len(contract_publication_result) == 2
    assert not contract_publication_result.has_errors()

    assert contract_publication_result[0].contract.data_source_name == 'test'
    assert contract_publication_result[1].contract.data_source_name == 'test2'
