from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, ContractYaml
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_parse_relative_complete_contract():
    contract_yaml_source: YamlSource = YamlSource.from_str(
        yaml_str=dedent_and_strip(
            """
        dataset: SODATEST_test_schema_31761d69
        dataset_prefix: [soda_test, dev_xxx]
        columns:
          - name: id
            data_type: varchar(255)
            checks:
              - missing:
        checks:
          - schema:
        """
        )
    )

    contract_yaml: ContractYaml = ContractYaml.parse(contract_yaml_source=contract_yaml_source, variables={})

    assert ["soda_test", "dev_xxx"] == contract_yaml.dataset_prefix
    assert "SODATEST_test_schema_31761d69" == contract_yaml.dataset

    column_yaml: ColumnYaml = contract_yaml.columns[0]
    assert "id" == column_yaml.name
    assert "varchar(255)" == column_yaml.data_type

    check_yaml: CheckYaml = contract_yaml.checks[0]
    assert check_yaml.__class__.__name__ == "SchemaCheckYaml"


def test_parse_minimal_contract():
    logs: Logs = Logs()
    ContractYaml.parse(
        contract_yaml_source=YamlSource.from_str(
            """
            data_source: abc
            dataset: dsname
        """
        )
    )
    logs.remove_from_root_logger()

    assert not logs.get_errors_str()
