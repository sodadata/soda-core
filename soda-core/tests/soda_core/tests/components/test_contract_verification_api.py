from soda_core.contracts.contract_verification import ContractVerificationResult, ContractVerification


def test_contract_verification_api():
    variables: dict = {}

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_data_source_yaml_file("path_to_ds_file")
        .with_contract_yaml_file("path_to_contract_file")
        .with_variables(variables)
        .execute()
    )

    contract_verification_result.contract_results
