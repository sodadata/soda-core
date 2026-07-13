"""
Tests for DuplicateCheckYaml parsing.

Two variants:
- ColumnDuplicateCheckYaml: column-level, extends MissingAncValidityCheckYaml
- MultiColumnDuplicateCheckYaml: dataset-level with columns field, extends ThresholdCheckYaml

Common fields (name, filter, threshold, metric, attributes) are tested in test_common_check_yaml_features.py.
"""

from helpers.yaml_parsing_helpers import parse_check_from_contract, parse_column_check


class TestColumnDuplicateCheckYamlParsing:
    def test_column_duplicate(self):
        """Column-level duplicate check is recognized."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - duplicate:
        """
        check = parse_column_check(yaml_str)
        assert check.type_name == "duplicate"
        assert check.threshold is None


class TestMultiColumnDuplicateCheckYamlParsing:
    def test_multi_column_duplicate_parses_columns(self):
        """Multi-column duplicate check parses the columns list."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - duplicate:
                  columns:
                    - col1
                    - col2
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "duplicate"
        assert check.columns == ["col1", "col2"]
        assert check.name is None
        assert check.filter is None
        assert check.threshold is None
