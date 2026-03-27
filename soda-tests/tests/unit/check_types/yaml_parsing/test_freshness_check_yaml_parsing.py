"""
Tests for FreshnessCheckYaml parsing.

Focuses on unique fields:
- column: required dataset column name
- now_variable: optional variable name for current timestamp
- unit: parsed from threshold block, valid values are "minute", "hour", "day"
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Dataset-level only

Common fields (name, filter, threshold, metric, attributes) are tested in test_common_check_yaml_features.py.
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestFreshnessCheckYamlParsing:
    def test_freshness_parses_all_unique_fields(self):
        """Freshness check parses column, now_variable, and unit."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  column: created_at
                  now_variable: "CUSTOM_NOW"
                  threshold:
                    unit: day
                    must_be_less_than: 7
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "freshness"
        assert check.column == "created_at"
        assert check.now_variable == "CUSTOM_NOW"
        assert check.unit == "day"
        assert check.name is None
        assert check.filter is None
