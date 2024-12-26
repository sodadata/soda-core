from soda_core.common.data_source import DataSource
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractVerification


def test_contract_verification_file_api():
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_file("./path/to/contract/file.yml")
        .with_variables({"env": "test"})
        .execute()
    )

    contract_verification_result.contract_results


def test_contract_verification_spark_session():
    spark_session = "spark-session"

    spark_data_source: DataSource = DataSource.from_dict(
        data_source_yaml_dict={
            type: "spark-df"
        },
        spark_session=spark_session
    )

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder(provided_data_source=spark_data_source)
        .with_contract_yaml_file("./path/to/contract/file.yml")
        .with_variables({"env": "test"})
        .execute()
    )

    contract_verification_result.contract_results
