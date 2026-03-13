"""
Tests for MissingCheckYaml parsing.

Focuses on unique fields:
- missing_values: list of values to treat as missing
- missing_format: regex format for missing values (with regex and name sub-fields)
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Column-level only

Common fields (name, filter, threshold, metric, attributes) are tested in test_common_check_yaml_features.py.
"""


from helpers.yaml_parsing_helpers import parse_column_check


class TestMissingCheckYamlParsing:
    def test_missing_parses_all_unique_fields(self):
        """Missing check parses missing_values and missing_format together."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_column
                checks:
                  - missing:
                      missing_values: ["N/A", "null"]
                      missing_format:
                        regex: "^[Nn]ull$"
                        name: "null_variant"
        """
        check = parse_column_check(yaml_str)
        assert check.type_name == "missing"
        assert check.missing_values == ["N/A", "null"]
        assert check.missing_format is not None
        assert check.missing_format.regex == "^[Nn]ull$"
        assert check.missing_format.name == "null_variant"
        assert check.name is None
        assert check.filter is None
        assert check.threshold is None
