"""
Tests for MissingCheckYaml parsing.

Focuses on unique fields:
- Extends MissingAncValidityCheckYaml (inherits missing_values, missing_format, metric, unit, threshold)
- Column-level only
"""


from helpers.yaml_parsing_helpers import parse_column_check


class TestMissingCheckYamlParsing:
    def test_missing_default(self):
        """Missing check with basic structure."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - missing:
                      threshold:
                        must_be_less_than_or_equal: 5
        """
        check = parse_column_check(yaml_str)
        assert check.type_name == "missing"
        assert check.threshold is not None
        assert check.threshold.must_be_less_than_or_equal == 5

    def test_missing_with_metric_percent(self):
        """Missing check with percent metric."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - missing:
                      threshold:
                        metric: percent
                        must_be_less_than: 10
        """
        check = parse_column_check(yaml_str)
        assert check.metric == "percent"
        assert check.threshold.must_be_less_than == 10

    def test_missing_with_missing_values(self):
        """Missing check with specific missing values."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - missing:
                      missing_values: ["N/A", "null"]
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.missing_values == ["N/A", "null"]

    def test_missing_with_missing_format(self):
        """Missing check with regex format for missing values."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - missing:
                      missing_format:
                        regex: "^[Nn]ull$"
                        name: "null_variant"
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.missing_format is not None
        assert check.missing_format.regex == "^[Nn]ull$"
        assert check.missing_format.name == "null_variant"

    def test_missing_with_name(self):
        """Missing check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - missing:
                      name: "Check for missing values"
                      threshold:
                        must_be_less_than: 5
        """
        check = parse_column_check(yaml_str)
        assert check.name == "Check for missing values"

    def test_missing_with_filter(self):
        """Missing check with filter condition."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - missing:
                      filter: "status = 'active'"
                      threshold:
                        must_be_less_than: 10
        """
        check = parse_column_check(yaml_str)
        assert check.filter == "status = 'active'"
