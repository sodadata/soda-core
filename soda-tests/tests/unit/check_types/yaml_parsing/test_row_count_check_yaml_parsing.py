"""
Tests for RowCountCheckYaml parsing.

Focuses on unique fields:
- Extends ThresholdCheckYaml (inherits metric, unit, threshold)
- No column-specific behavior (dataset-level only)
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestRowCountCheckYamlParsing:
    def test_row_count_default(self):
        """Row count check with basic structure."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be_greater_than: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "row_count"
        assert check.threshold is not None
        assert check.threshold.must_be_greater_than == 0

    def test_row_count_with_filter(self):
        """Row count check with filter condition."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  filter: "status = 'active'"
                  threshold:
                    must_be_greater_than: 100
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "row_count"
        assert check.filter == "status = 'active'"
        assert check.threshold.must_be_greater_than == 100

    def test_row_count_with_name(self):
        """Row count check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  name: "Check at least 1000 rows"
                  threshold:
                    must_be_greater_than_or_equal: 1000
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "Check at least 1000 rows"
        assert check.threshold.must_be_greater_than_or_equal == 1000

    def test_row_count_with_metric_percent(self):
        """Row count with percent metric."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    metric: percent
                    must_be_greater_than: 50
        """
        check = parse_check_from_contract(yaml_str)
        assert check.metric == "percent"
        assert check.threshold.must_be_greater_than == 50

    def test_row_count_with_level_warn(self):
        """Row count with warn level threshold."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    level: warn
                    must_be_greater_than: 10
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.level == "warn"
        assert check.threshold.must_be_greater_than == 10

    def test_row_count_with_attributes(self):
        """Row count check with custom attributes."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  attributes:
                    owner: "data_team"
                    priority: "high"
                  threshold:
                    must_be_greater_than: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.attributes == {"owner": "data_team", "priority": "high"}
