from soda_core.common.data_source import DataSource
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.yaml import YamlSource
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractVerification
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper


def test_contract_verification_file_api():
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
        .with_variables({"env": "test"})
        .execute()
    )

    assert "Contract file ../soda/mydb/myschema/table.yml does not exist" in str(contract_verification_result)


def test_contract_provided_and_configured(data_source_test_helper: DataSourceTestHelper):
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder(data_source_test_helper.data_source)
        .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
        .with_variables({"env": "test"})
        .execute()
    )

    assert "Contract file ../soda/mydb/myschema/table.yml does not exist" in str(contract_verification_result)


def test_contract_verification_spark_session():
    spark_session = "spark-session"

    spark_data_source: DataSource = DataSourceParser(
        data_source_yaml_source=YamlSource.from_dict(yaml_dict={
            type: "spark-df"
        }),
        spark_session=spark_session
    ).parse()

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder(provided_data_source=spark_data_source)
        .with_contract_yaml_dict({
            "data_source_file": "./path/to/file.yml"
        })
        .with_variables({"env": "test"})
        .execute()
    )

    assert "Contract file ../soda/mydb/myschema/table.yml does not exist" in str(contract_verification_result)
