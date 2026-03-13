"""
Tests for InvalidCheckYaml parsing.

Focuses on unique fields:
- invalid_values, invalid_format (regex + name)
- valid_values, valid_format (regex + name)
- valid_min, valid_max
- valid_reference_data (dataset + column)
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Column-level only

Common fields (name, filter, threshold, metric, attributes) are tested in test_common_check_yaml_features.py.
"""


from helpers.yaml_parsing_helpers import parse_column_check


class TestInvalidCheckYamlParsing:
    def test_invalid_parses_values_formats_and_ranges(self):
        """Invalid check parses all value, format, and range fields together."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      invalid_values: ["bad", "wrong"]
                      invalid_format:
                        regex: "^[Xx][Xx]{2}$"
                        name: "invalid_pattern"
                      valid_values: ["yes", "no"]
                      valid_format:
                        regex: "^[0-9]{3}-[0-9]{4}$"
                        name: "phone_format"
                      valid_min: 0
                      valid_max: 100
        """
        check = parse_column_check(yaml_str)
        assert check.type_name == "invalid"
        assert check.invalid_values == ["bad", "wrong"]
        assert check.invalid_format.regex == "^[Xx][Xx]{2}$"
        assert check.invalid_format.name == "invalid_pattern"
        assert check.valid_values == ["yes", "no"]
        assert check.valid_format.regex == "^[0-9]{3}-[0-9]{4}$"
        assert check.valid_format.name == "phone_format"
        assert check.valid_min == 0
        assert check.valid_max == 100
        assert check.name is None
        assert check.filter is None
        assert check.threshold is None

    def test_invalid_parses_valid_reference_data(self):
        """Invalid check parses valid_reference_data with dataset and column."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - invalid:
                      valid_reference_data:
                        dataset: schema/ref_table
                        column: "ref_column"
        """
        check = parse_column_check(yaml_str)
        assert check.valid_reference_data is not None
        assert check.valid_reference_data.dataset == "schema/ref_table"
        assert check.valid_reference_data.column == "ref_column"
