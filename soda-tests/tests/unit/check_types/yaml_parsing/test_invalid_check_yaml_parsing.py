"""
Tests for InvalidCheckYaml parsing (invalidity_check_yaml.py).

Focuses on unique fields:
- Extends MissingAncValidityCheckYaml (inherits invalid_values, invalid_format, metric, unit, threshold)
- Also inherits valid_values, valid_format, valid_min, valid_max, etc.
- Column-level only
"""


from helpers.yaml_parsing_helpers import parse_column_check


class TestInvalidCheckYamlParsing:
    def test_invalid_with_invalid_values(self):
        """Invalid check with invalid_values list."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      invalid_values: ["bad", "wrong"]
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.type_name == "invalid"
        assert check.invalid_values == ["bad", "wrong"]
        assert check.threshold.must_be_less_than_or_equal == 0

    def test_invalid_with_valid_values(self):
        """Invalid check with valid_values (complementary to invalid_values)."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      valid_values: ["yes", "no"]
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.valid_values == ["yes", "no"]

    def test_invalid_with_valid_format(self):
        """Invalid check with regex format for valid values."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      valid_format:
                        regex: "^[0-9]{3}-[0-9]{4}$"
                        name: "phone_format"
                      threshold:
                        must_be_less_than: 5
        """
        check = parse_column_check(yaml_str)
        assert check.valid_format is not None
        assert check.valid_format.regex == "^[0-9]{3}-[0-9]{4}$"
        assert check.valid_format.name == "phone_format"

    def test_invalid_with_valid_reference_data(self):
        """Invalid check with valid_reference_data."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      valid_reference_data:
                        dataset: schema/ref_table
                        column: "ref_column"
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.valid_reference_data is not None
        assert check.valid_reference_data.dataset == "schema/ref_table"
        assert check.valid_reference_data.column == "ref_column"

    def test_invalid_with_metric_percent(self):
        """Invalid check with percent metric."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      invalid_values: ["bad"]
                      threshold:
                        metric: percent
                        must_be_less_than: 5
        """
        check = parse_column_check(yaml_str)
        assert check.metric == "percent"
        assert check.threshold.must_be_less_than == 5

    def test_invalid_with_invalid_format(self):
        """Invalid check with regex format for invalid values."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      invalid_format:
                        regex: "^[Xx][Xx]{2}$"
                        name: "invalid_pattern"
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.invalid_format is not None
        assert check.invalid_format.regex == "^[Xx][Xx]{2}$"
        assert check.invalid_format.name == "invalid_pattern"

    def test_invalid_with_valid_min_max(self):
        """Invalid check with valid_min and valid_max."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      valid_min: 0
                      valid_max: 100
                      threshold:
                        must_be_less_than_or_equal: 0
        """
        check = parse_column_check(yaml_str)
        assert check.valid_min == 0
        assert check.valid_max == 100
