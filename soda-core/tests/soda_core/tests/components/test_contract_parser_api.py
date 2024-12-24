from soda_core.common.yaml import YamlSource
from soda_core.contracts.impl.contract_yaml import ContractYaml, ColumnYaml, CheckYaml


def test_contract_parser():
    contract_yaml_file: YamlSource = YamlSource(
        yaml_str="""
        data_source: postgres_test_ds
        database: soda_test
        schema: dev_tom
        dataset: SODATEST_test_schema_31761d69
        columns:
          - name: id
            data_type: varchar(255)
        checks:
          - type: schema
        """
    )
    contract_yaml_file.parse({})

    contract: ContractYaml = ContractYaml(contract_yaml_file=contract_yaml_file)

    assert "postgres_test_ds" == contract.data_source_file
    assert "soda_test" == contract.database_name
    assert "dev_tom" == contract.schema_name
    assert "SODATEST_test_schema_31761d69" == contract.dataset_name

    column: ColumnYaml = contract.columns[0]
    assert "id" == column.name
    assert "varchar(255)" == column.data_type

    check: CheckYaml = contract.checks[0]
    assert check.__class__.__name__ == "SchemaCheckYaml"
