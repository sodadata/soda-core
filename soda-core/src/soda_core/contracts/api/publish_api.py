from typing import Optional

from soda_core.contracts.contract_publication import (
    ContractPublication,
    ContractPublicationResultList,
)


def publish_contracts(
    contract_file_paths: Optional[list[str]], soda_cloud_file_path: Optional[str]
) -> ContractPublicationResultList:
    contract_publication_builder = ContractPublication.builder()

    for contract_file_path in contract_file_paths:
        contract_publication_builder.with_contract_yaml_file(contract_file_path)

    if soda_cloud_file_path:
        contract_publication_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)

    contract_publication_result = contract_publication_builder.build().execute()

    return contract_publication_result
