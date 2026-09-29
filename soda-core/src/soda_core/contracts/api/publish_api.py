from soda_core.contracts.contract_publication import ContractPublication, ContractPublicationResultList
from soda_core.telemetry.soda_telemetry import SodaTelemetry

soda_telemetry = SodaTelemetry()


def publish_contract(contract_file_path: str, soda_cloud_file_path: str) -> ContractPublicationResultList:
    contract_publication_builder = ContractPublication.builder()

    contract_publication_builder.with_contract_yaml_file(contract_file_path)
    contract_publication_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)

    contract_publication: ContractPublication = contract_publication_builder.build()
    contract_publication_result = contract_publication.execute()

    for contract_yaml in contract_publication.contract_publication_impl.contract_yamls:
        soda_telemetry.ingest_contract_publication(contract_yaml)

    return contract_publication_result
