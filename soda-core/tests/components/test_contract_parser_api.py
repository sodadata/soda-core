import pytest
from helpers.test_functions import dedent_and_strip
from soda_core.common.exceptions import InvalidDatasetQualifiedNameException
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, ContractYaml


def test_parse_relative_complete_contract():
    contract_yaml_source: ContractYamlSource = ContractYamlSource.from_str(
        yaml_str=dedent_and_strip(
            """
        dataset: sdf/soda_test/dev_xxx/SODATEST_test_schema_31761d69
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

    contract_yaml: ContractYaml = ContractYaml.parse(
        contract_yaml_source=contract_yaml_source, provided_variable_values={}
    )

    assert "sdf/soda_test/dev_xxx/SODATEST_test_schema_31761d69" == contract_yaml.dataset

    column_yaml: ColumnYaml = contract_yaml.columns[0]
    assert "id" == column_yaml.name
    assert "varchar(255)" == column_yaml.data_type

    check_yaml: CheckYaml = contract_yaml.checks[0]
    assert check_yaml.__class__.__name__ == "SchemaCheckYaml"


def test_legacy_dataset_specification():
    with pytest.raises(
        InvalidDatasetQualifiedNameException, match="Identifier must contain at least a data source and a dataset"
    ):
        ContractYaml.parse(
            contract_yaml_source=ContractYamlSource.from_str(
                """
                data_source: abc
                dataset_prefix: [a, b]
                dataset: dsname
            """
            )
        )


def test_minimal_contract():
    logs: Logs = Logs()
    ContractYaml.parse(
        contract_yaml_source=ContractYamlSource.from_str(
            """
            dataset: a/b/c/d
        """
        )
    )
    logs.remove_from_root_logger()

    assert not logs.get_errors_str()
