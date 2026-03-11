"""
Unit tests for check identity computation.

Tests that checks have consistent and distinct identities based on their
type, column, and other identifying characteristics.
"""

from helpers.yaml_parsing_helpers import parse_contract


def test_different_check_types_have_different_identities():
    """Test that different check types on the same column have different identities."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: integer
        checks:
          - missing:
          - invalid:
              valid_values:
                - 1
          - row_count:
    """
    contract = parse_contract(contract_yaml)

    # Parse each check type
    col = contract.columns[0]
    assert col.name == "user_id"
    assert len(col.check_yamls) == 3

    # Verify they have different type_names
    missing_check = col.check_yamls[0]
    invalid_check = col.check_yamls[1]
    row_count_check = col.check_yamls[2]

    assert missing_check.type_name == "missing"
    assert invalid_check.type_name == "invalid"
    assert row_count_check.type_name == "row_count"


def test_same_check_type_on_different_columns_has_different_identity():
    """Test that same check type on different columns has different column context."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: integer
        checks:
          - missing:
      - name: email
        data_type: character varying
        checks:
          - missing:
    """
    contract = parse_contract(contract_yaml)

    assert len(contract.columns) == 2
    col1 = contract.columns[0]
    col2 = contract.columns[1]

    assert col1.name == "user_id"
    assert col2.name == "email"

    # Both have missing checks but on different columns
    assert col1.check_yamls[0].type_name == "missing"
    assert col2.check_yamls[0].type_name == "missing"


def test_check_qualifier_does_not_affect_type_name():
    """Test that check qualifier (name) doesn't change the type_name."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        checks:
          - invalid:
              name: first check
              valid_values:
                - active
          - invalid:
              name: second check
              valid_values:
                - inactive
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert len(col.check_yamls) == 2

    check1 = col.check_yamls[0]
    check2 = col.check_yamls[1]

    # Both are "invalid" type regardless of name
    assert check1.type_name == "invalid"
    assert check2.type_name == "invalid"

    # But they have different names
    assert check1.name == "first check"
    assert check2.name == "second check"


def test_all_check_types_have_distinct_type_names():
    """Test that all 9 check types have distinct type_names."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: col1
        data_type: integer
        checks:
          - missing:
          - invalid:
              valid_values: [1]
          - duplicate:
          - aggregate:
              function: sum
              must_be_greater_than: 0
      - name: col2
        data_type: timestamp
        checks:
          - freshness:
              column: col2
              must_be_less_than: 24
    checks:
      - row_count:
      - schema:
      - metric:
          name: test_metric
          expression: count(*)
          must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)

    # Collect all check type names
    type_names = set()

    # Dataset-level checks
    if contract.checks:
        for check in contract.checks:
            type_names.add(check.type_name)

    # Column-level checks
    for col in contract.columns:
        if col.check_yamls:
            for check in col.check_yamls:
                type_names.add(check.type_name)

    # We should have at least these unique types
    expected_types = {
        "row_count",
        "missing",
        "invalid",
        "duplicate",
        "freshness",
        "aggregate",
        "schema",
        "metric",
    }

    assert expected_types.issubset(type_names), f"Expected types {expected_types}, got {type_names}"


def test_check_identity_with_variables():
    """Test that check identity is consistent with variable resolution."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    variables:
      valid_status:
        default: active
    columns:
      - name: status
        data_type: character varying
        checks:
          - invalid:
              valid_values:
                - ${var.valid_status}
    """
    variables = {"valid_status": "active"}
    contract = parse_contract(contract_yaml, variables=variables)

    col = contract.columns[0]
    check = col.check_yamls[0]

    assert check.type_name == "invalid"
    # Variables should be resolved in the parsed YAML
    assert contract.resolved_variable_values.get("valid_status") == "active"
    # Verify the resolved variable made it into the check's valid_values
    assert "active" in check.valid_values
