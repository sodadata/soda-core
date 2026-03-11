"""
Tests for FreshnessCheckYaml parsing.

Focuses on unique fields:
- column: required dataset column name
- now_variable: optional variable name for current timestamp
- unit: valid values are "minute", "hour", "day"
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Dataset-level only (errors if in column checks)
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestFreshnessCheckYamlParsing:
    def test_freshness_basic(self):
        """Freshness check with column and unit."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  column: created_at
                  threshold:
                    unit: day
                    must_be_less_than_or_equal: 7
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "freshness"
        assert check.column == "created_at"
        assert check.unit == "day"
        assert check.threshold.must_be_less_than_or_equal == 7

    def test_freshness_with_unit_hour(self):
        """Freshness check with hour unit."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  column: updated_at
                  threshold:
                    unit: hour
                    must_be_less_than: 24
        """
        check = parse_check_from_contract(yaml_str)
        assert check.unit == "hour"
        assert check.threshold.must_be_less_than == 24

    def test_freshness_with_unit_minute(self):
        """Freshness check with minute unit."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  column: last_sync
                  threshold:
                    unit: minute
                    must_be_less_than_or_equal: 60
        """
        check = parse_check_from_contract(yaml_str)
        assert check.unit == "minute"

    def test_freshness_with_now_variable(self):
        """Freshness check with now_variable override."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  column: created_at
                  now_variable: "CUSTOM_NOW"
                  threshold:
                    unit: day
                    must_be_less_than: 5
        """
        check = parse_check_from_contract(yaml_str)
        assert check.now_variable == "CUSTOM_NOW"

    def test_freshness_with_metric_count(self):
        """Freshness check with count metric."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  column: last_modified
                  threshold:
                    metric: count
                    unit: hour
                    must_be_less_than: 12
        """
        check = parse_check_from_contract(yaml_str)
        assert check.metric == "count"
        assert check.unit == "hour"

    def test_freshness_with_filter(self):
        """Freshness check with filter condition."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  column: processed_at
                  filter: "status = 'completed'"
                  threshold:
                    unit: day
                    must_be_less_than: 3
        """
        check = parse_check_from_contract(yaml_str)
        assert check.filter == "status = 'completed'"
        assert check.column == "processed_at"

    def test_freshness_with_name(self):
        """Freshness check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - freshness:
                  name: "Data must be fresh"
                  column: updated_at
                  threshold:
                    unit: day
                    must_be_less_than_or_equal: 1
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "Data must be fresh"
