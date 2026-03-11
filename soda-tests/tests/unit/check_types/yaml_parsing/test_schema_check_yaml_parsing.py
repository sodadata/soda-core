"""
Tests for SchemaCheckYaml parsing.

Focuses on unique fields:
- allow_extra_columns: bool, whether extra columns are allowed
- allow_other_column_order: bool, whether column order can differ
- Extends CheckYaml (no threshold/metric)
- Dataset-level only
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestSchemaCheckYamlParsing:
    def test_schema_check_basic(self):
        """Schema check with basic structure."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
              - name: name
                data_type: VARCHAR
            checks:
              - schema
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "schema"

    def test_schema_check_allow_extra_columns_true(self):
        """Schema check allowing extra columns."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
            checks:
              - schema:
                  allow_extra_columns: true
        """
        check = parse_check_from_contract(yaml_str)
        assert check.allow_extra_columns is True

    def test_schema_check_allow_extra_columns_false(self):
        """Schema check disallowing extra columns."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
            checks:
              - schema:
                  allow_extra_columns: false
        """
        check = parse_check_from_contract(yaml_str)
        assert check.allow_extra_columns is False

    def test_schema_check_allow_other_column_order_true(self):
        """Schema check allowing different column order."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
              - name: name
                data_type: VARCHAR
            checks:
              - schema:
                  allow_other_column_order: true
        """
        check = parse_check_from_contract(yaml_str)
        assert check.allow_other_column_order is True

    def test_schema_check_allow_other_column_order_false(self):
        """Schema check requiring exact column order."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
              - name: name
                data_type: VARCHAR
            checks:
              - schema:
                  allow_other_column_order: false
        """
        check = parse_check_from_contract(yaml_str)
        assert check.allow_other_column_order is False

    def test_schema_check_with_both_flags(self):
        """Schema check with both allow flags."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
            checks:
              - schema:
                  allow_extra_columns: true
                  allow_other_column_order: true
        """
        check = parse_check_from_contract(yaml_str)
        assert check.allow_extra_columns is True
        assert check.allow_other_column_order is True

    def test_schema_check_with_name(self):
        """Schema check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
            checks:
              - schema:
                  name: "Validate schema structure"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "Validate schema structure"

    def test_schema_check_with_qualifier(self):
        """Schema check with qualifier."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
            checks:
              - schema:
                  qualifier: "exact_match"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.qualifier == "exact_match"
