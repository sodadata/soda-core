from helpers.test_functions import dedent_and_strip
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_yaml import CheckYaml, ContractYaml


def _parse_contract(yaml_str: str) -> ContractYaml:
    return ContractYaml.parse(
        contract_yaml_source=ContractYamlSource.from_str(yaml_str=dedent_and_strip(yaml_str)),
        provided_variable_values={},
    )


def test_sql_remediation_stored_as_dict():
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
    assert isinstance(check_yaml.remediation, dict)
    assert check_yaml.remediation["description"] == "Map invalid country codes to ISO standard"
    assert check_yaml.remediation["strategy"]["type"] == "sql"
    assert "SELECT iso_code" in check_yaml.remediation["strategy"]["query"]


def test_llm_remediation_stored_as_dict():
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
    assert isinstance(remediation, dict)
    assert remediation["description"] == "Look up email from customer directory"
    assert remediation["strategy"]["type"] == "llm"
    assert remediation["strategy"]["prompt"] == "Look up the correct email address for this customer."
    assert len(remediation["strategy"]["references"]) == 2
    assert remediation["strategy"]["references"][0]["name"] == "customer_directory"


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
    assert check_yaml.remediation["description"] == "Re-run the ETL pipeline"
    assert check_yaml.remediation["strategy"]["type"] == "sql"
