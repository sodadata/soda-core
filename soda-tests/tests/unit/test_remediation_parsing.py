from helpers.test_functions import dedent_and_strip
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_yaml import CheckYaml, ContractYaml


def _parse_contract(yaml_str: str) -> ContractYaml:
    return ContractYaml.parse(
        contract_yaml_source=ContractYamlSource.from_str(yaml_str=dedent_and_strip(yaml_str)),
        provided_variable_values={},
    )


def test_sql_remediation_parsing():
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: country_code
            data_type: VARCHAR
            checks:
              - invalid:
                  name: "Country code validity"
                  valid_values: ['US', 'GB', 'DE']
                  remediation:
                    description: "Map invalid country codes to ISO standard"
                    strategy:
                      type: sql
                      query: |
                        SELECT iso_code FROM country_mappings WHERE legacy_code = '{value}' LIMIT 1
        """
    )

    check_yaml: CheckYaml = contract_yaml.columns[0].check_yamls[0]
    assert check_yaml.remediation is not None
    assert check_yaml.remediation.description == "Map invalid country codes to ISO standard"
    assert check_yaml.remediation.strategy.type == "sql"
    assert "SELECT iso_code" in check_yaml.remediation.strategy.query
    assert check_yaml.remediation.strategy.prompt is None
    assert check_yaml.remediation.strategy.references is None
    assert check_yaml.remediation.strategy.tools is None


def test_llm_remediation_parsing():
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: email
            data_type: VARCHAR
            checks:
              - missing:
                  name: "Email completeness"
                  remediation:
                    description: "Look up email from customer directory"
                    strategy:
                      type: llm
                      prompt: "Look up the correct email address for this customer."
                      references:
                        - type: table
                          name: customer_directory
                          description: "Master customer directory with verified emails"
                        - type: column
                          name: customers.name
                      tools:
                        - name: email_validator
                          description: "Validates email format and deliverability"
        """
    )

    check_yaml: CheckYaml = contract_yaml.columns[0].check_yamls[0]
    remediation = check_yaml.remediation
    assert remediation is not None
    assert remediation.description == "Look up email from customer directory"
    assert remediation.strategy.type == "llm"
    assert remediation.strategy.prompt == "Look up the correct email address for this customer."
    assert remediation.strategy.query is None

    assert len(remediation.strategy.references) == 2
    assert remediation.strategy.references[0].type == "table"
    assert remediation.strategy.references[0].name == "customer_directory"
    assert remediation.strategy.references[0].description == "Master customer directory with verified emails"
    assert remediation.strategy.references[1].type == "column"
    assert remediation.strategy.references[1].name == "customers.name"
    assert remediation.strategy.references[1].description is None

    assert len(remediation.strategy.tools) == 1
    assert remediation.strategy.tools[0].name == "email_validator"
    assert remediation.strategy.tools[0].description == "Validates email format and deliverability"


def test_remediation_is_optional():
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: id
            data_type: VARCHAR
            checks:
              - missing:
                  name: "ID completeness"
        """
    )

    check_yaml: CheckYaml = contract_yaml.columns[0].check_yamls[0]
    assert check_yaml.remediation is None


def test_invalid_strategy_type_produces_error(logs: Logs):
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: id
            data_type: VARCHAR
            checks:
              - missing:
                  remediation:
                    description: "Fix it"
                    strategy:
                      type: unknown_strategy
        """
    )

    check_yaml: CheckYaml = contract_yaml.columns[0].check_yamls[0]
    assert check_yaml.remediation is None
    assert "Unknown remediation strategy type" in logs.get_errors_str()


def test_llm_remediation_without_references_and_tools():
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: phone
            data_type: VARCHAR
            checks:
              - invalid:
                  name: "Phone format"
                  valid_values: ['+1']
                  remediation:
                    description: "Reformat phone numbers"
                    strategy:
                      type: llm
                      prompt: "Reformat this phone number to E.164 format."
        """
    )

    check_yaml: CheckYaml = contract_yaml.columns[0].check_yamls[0]
    remediation = check_yaml.remediation
    assert remediation is not None
    assert remediation.strategy.type == "llm"
    assert remediation.strategy.prompt == "Reformat this phone number to E.164 format."
    assert remediation.strategy.references is None
    assert remediation.strategy.tools is None


def test_remediation_without_description_returns_none(logs: Logs):
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: id
            checks:
              - missing:
                  remediation:
                    strategy:
                      type: sql
                      query: "SELECT 1"
        """
    )

    check_yaml: CheckYaml = contract_yaml.columns[0].check_yamls[0]
    assert check_yaml.remediation is None


def test_remediation_without_strategy_returns_none(logs: Logs):
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: id
            checks:
              - missing:
                  remediation:
                    description: "Fix the data"
        """
    )

    check_yaml: CheckYaml = contract_yaml.columns[0].check_yamls[0]
    assert check_yaml.remediation is None


def test_dataset_level_check_with_remediation():
    contract_yaml = _parse_contract(
        """
        dataset: ds/schema/table
        columns:
          - name: id
        checks:
          - row_count:
              name: "Row count check"
              threshold:
                must_be_greater_than: 0
              remediation:
                description: "Re-run the ETL pipeline"
                strategy:
                  type: sql
                  query: "SELECT 1"
        """
    )

    check_yaml: CheckYaml = contract_yaml.checks[0]
    assert check_yaml.remediation is not None
    assert check_yaml.remediation.description == "Re-run the ETL pipeline"
    assert check_yaml.remediation.strategy.type == "sql"
