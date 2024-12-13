from soda_core.common.yaml import YamlFile
from soda_core.contracts.impl.contract_yaml import ContractYaml, ColumnYaml, CheckYaml
from soda_core.contracts.impl.contract_parser import ContractParser


def test_contract_parser():
    contract_yaml_file: YamlFile = YamlFile(
        yaml_str="""
        data_source: postgres_test_ds
        database: soda_test
        schema: dev_tom
        dataset: SODATEST_test_schema_31761d69
        columns:
          - name: id
            data_type: varchar(255)
            checks:
              - type: missing
        """
    )

    contract_parser: ContractParser = ContractParser(contract_yaml_file=contract_yaml_file)
    contract: ContractYaml = contract_parser.parse()

    assert "postgres_test_ds" == contract.data_source_name
    assert "soda_test" == contract.database_name
    assert "dev_tom" == contract.schema_name
    assert "SODATEST_test_schema_31761d69" == contract.dataset_name

    column: ColumnYaml = contract.columns[0]
    assert "id" == column.name
    assert "varchar(255)" == column.data_type

    check: CheckYaml = column.checks[0]
    assert type(check).__name__ == "MissingCheck"
