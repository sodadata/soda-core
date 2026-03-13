"""
Tests for SchemaCheckYaml parsing.

Focuses on unique fields:
- allow_extra_columns: bool, whether extra columns are allowed
- allow_other_column_order: bool, whether column order can differ
- Extends CheckYaml (no threshold/metric)
- Dataset-level only

Common fields (name, qualifier, filter, attributes) are tested in test_common_check_yaml_features.py.
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestSchemaCheckYamlParsing:
    def test_schema_defaults(self):
        """Schema check without flags has correct defaults."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
            checks:
              - schema:
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "schema"
        assert check.allow_extra_columns is None
        assert check.allow_other_column_order is None
        assert check.name is None
        assert check.qualifier is None

    def test_schema_with_both_flags(self):
        """Schema check parses both allow flags."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: id
                data_type: INTEGER
            checks:
              - schema:
                  allow_extra_columns: true
                  allow_other_column_order: false
        """
        check = parse_check_from_contract(yaml_str)
        assert check.allow_extra_columns is True
        assert check.allow_other_column_order is False
