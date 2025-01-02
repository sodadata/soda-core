from unittest import skip

import pytest

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.yaml import YamlSource
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractVerification, SodaException


def test_contract_verification_file_api():
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
        .with_variables({"env": "test"})
        .execute()
    )

    assert "Contract file '../soda/mydb/myschema/table.yml' does not exist" in str(contract_verification_result)


def test_contract_verification_file_api_exception_on_error():
    with pytest.raises(SodaException) as e:
        contract_verification_result: ContractVerificationResult = (
            ContractVerification.builder()
            .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
            .with_variables({"env": "test"})
            .execute()
            .assert_ok()
        )

    exception_string = str(e.value)
    assert "Contract file '../soda/mydb/myschema/table.yml' does not exist" in exception_string


def test_contract_provided_and_configured():
    """
    If there is no default data source configured and there is none provided in the contract, an error has to be logged
    """
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_str(f"""
          dataset: CUSTOMERS
          columns:
            - name: id
        """)
        .with_variables({"env": "test"})
        .execute()
    )

    assert (("'data_source_file' is required. "
            "No default data source was configured in the contract verification builder.")
            in str(contract_verification_result))


@skip
def test_contract_verification_spark_session():
    spark_session = "spark-session"

    spark_data_source: DataSource = DataSourceParser(
        data_source_yaml_source=YamlSource.from_dict(yaml_dict={
            type: "spark-df"
        }),
        spark_session=spark_session
    ).parse()

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder(default_data_source=spark_data_source)
        .with_contract_yaml_dict({
            "dataset": "customers",
            "columns": [
                { "name": "id" }
            ]
        })
        .with_variables({"env": "test"})
        .execute()
    )
