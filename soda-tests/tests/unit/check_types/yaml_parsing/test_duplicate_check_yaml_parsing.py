"""
Tests for DuplicateCheckYaml parsing.

Focuses on unique fields:
- ColumnDuplicateCheckYaml extends MissingAncValidityCheckYaml (column-level)
- MultiColumnDuplicateCheckYaml extends ThresholdCheckYaml (dataset-level) with columns field
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract, parse_column_check


class TestColumnDuplicateCheckYamlParsing:
    def test_column_duplicate_basic(self):
        """Column-level duplicate check with basic structure."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - duplicate:
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.type_name == "duplicate"
        assert check.threshold is not None
        assert check.threshold.must_be_less_than_or_equal == 0

    def test_column_duplicate_with_metric_percent(self):
        """Column-level duplicate check with percent metric."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - duplicate:
                      threshold:
                        metric: percent
                        must_be_less_than: 5
        """
        check = parse_column_check(yaml_str)
        assert check.metric == "percent"


class TestMultiColumnDuplicateCheckYamlParsing:
    def test_multi_column_duplicate_basic(self):
        """Multi-column duplicate check with columns field."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - duplicate:
                  columns:
                    - col1
                    - col2
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "duplicate"
        assert check.columns == ["col1", "col2"]
        assert check.threshold.must_be_less_than_or_equal == 0

    def test_multi_column_duplicate_with_metric_count(self):
        """Multi-column duplicate check with count metric."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - duplicate:
                  columns:
                    - id
                    - email
                  threshold:
                    metric: count
                    must_be_less_than: 10
        """
        check = parse_check_from_contract(yaml_str)
        assert check.metric == "count"
        assert check.columns == ["id", "email"]

    def test_multi_column_duplicate_with_filter(self):
        """Multi-column duplicate check with filter."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - duplicate:
                  filter: "status = 'active'"
                  columns:
                    - user_id
                    - session_id
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.filter == "status = 'active'"
        assert check.columns == ["user_id", "session_id"]

    def test_multi_column_duplicate_with_name(self):
        """Multi-column duplicate check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - duplicate:
                  name: "Check no duplicate users"
                  columns:
                    - user_id
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "Check no duplicate users"
