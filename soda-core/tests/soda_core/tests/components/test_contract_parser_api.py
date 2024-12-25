from soda_core.common.yaml import YamlSource, YamlFileContent
from soda_core.contracts.impl.contract_yaml import ContractYaml, ColumnYaml, CheckYaml
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_contract_parser():
    contract_yaml_source: YamlSource = YamlSource.from_str(file_type="test_contract", yaml_str=dedent_and_strip("""
        data_source_file: ../../data_source_${env}.yml
        data_source_location:
          database: soda_test
          schema: dev_xxx
        dataset_name: SODATEST_test_schema_31761d69
        columns:
          - name: id
            data_type: varchar(255)
        checks:
          - type: schema
        """
    ))
    contract_yaml_file_content: YamlFileContent = contract_yaml_source.parse_yaml_file_content({"env": "test"})

    contract: ContractYaml = ContractYaml(contract_yaml_file_content=contract_yaml_file_content)

    assert "../../data_source_test.yml" == contract.data_source_file
    assert "soda_test" == contract.data_source_location.get("database")
    assert "dev_xxx" == contract.data_source_location.get("schema")
    assert "SODATEST_test_schema_31761d69" == contract.dataset_name

    column: ColumnYaml = contract.columns[0]
    assert "id" == column.name
    assert "varchar(255)" == column.data_type

    check: CheckYaml = contract.checks[0]
    assert check.__class__.__name__ == "SchemaCheckYaml"
