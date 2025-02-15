from soda_core.common.yaml import YamlSource
from soda_core.contracts.impl.contract_yaml import ContractYaml, ColumnYaml, CheckYaml
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_parse_relative_complete_contract():
    contract_yaml_source: YamlSource = YamlSource.from_str(yaml_str=dedent_and_strip("""
        data_source_file: ../../data_source_${env}.yml
        dataset_location_postgres:
          database: soda_test
          schema: dev_xxx
        dataset: SODATEST_test_schema_31761d69
        columns:
          - name: id
            data_type: varchar(255)
            checks:
              - type: missing
        checks:
          - type: schema
        """
    ))

    contract_yaml: ContractYaml = ContractYaml.parse(
        contract_yaml_source=contract_yaml_source, variables={"env": "test"}
    )

    assert "../../data_source_test.yml" == contract_yaml.data_source_file
    assert "soda_test" == contract_yaml.dataset_locations["postgres"]["database"]
    assert "dev_xxx" == contract_yaml.dataset_locations["postgres"]["schema"]
    assert "SODATEST_test_schema_31761d69" == contract_yaml.dataset_name

    column_yaml: ColumnYaml = contract_yaml.columns[0]
    assert "id" == column_yaml.name
    assert "varchar(255)" == column_yaml.data_type

    check_yaml: CheckYaml = contract_yaml.check_yamls[0]
    assert check_yaml.__class__.__name__ == "SchemaCheckYaml"


def test_parse_minimal_contract():
    contract_yaml: ContractYaml = ContractYaml.parse(contract_yaml_source=YamlSource.from_str("""
        dataset: customers
    """
    ))

    assert not contract_yaml.logs.has_errors()
