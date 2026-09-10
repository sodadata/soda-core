"""
Tests for AggregateCheckYaml parsing.

Focuses on the unique field:
- function: required aggregation function name
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Dataset-level only

Common fields (name, filter, threshold, metric, attributes) are tested in test_common_check_yaml_features.py.
"""

from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestAggregateCheckYamlParsing:
    def test_aggregate_parses_function(self):
        """Aggregate check parses its unique field: function."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  function: AVG
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "aggregate"
        assert check.function == "AVG"
        assert check.name is None
        assert check.filter is None
        assert check.threshold is None
