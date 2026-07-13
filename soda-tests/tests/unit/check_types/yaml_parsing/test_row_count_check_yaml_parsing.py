"""
Tests for RowCountCheckYaml parsing.

Row count has no unique config keys beyond what ThresholdCheckYaml provides.
This test verifies the type is recognized and defaults are correct.

Common fields (name, filter, threshold, metric, attributes) are tested in test_common_check_yaml_features.py.
"""

from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestRowCountCheckYamlParsing:
    def test_row_count_defaults(self):
        """Row count check parses with correct type and defaults."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "row_count"
        assert check.name is None
        assert check.qualifier is None
        assert check.filter is None
        assert check.attributes == {}
        assert check.threshold is None
        assert check.metric is None
        assert check.store_failed_rows is False
