"""
Tests for MetricCheckYaml parsing.

Focuses on unique fields:
- expression: optional custom metric expression
- query: optional custom SQL query
- Either expression or query is required (not both optional)
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Dataset-level only
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestMetricCheckYamlParsing:
    def test_metric_with_expression(self):
        """Metric check with expression."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  expression: "COUNT(*)"
                  threshold:
                    must_be_greater_than: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "metric"
        assert check.expression == "COUNT(*)"
        assert check.query is None

    def test_metric_with_query(self):
        """Metric check with query."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  query: |
                    SELECT COUNT(*) as value
                    FROM my_dataset
                    WHERE status = 'active'
                  threshold:
                    must_be_greater_than: 100
        """
        check = parse_check_from_contract(yaml_str)
        assert check.query is not None
        assert "COUNT(*)" in check.query
        assert check.expression is None

    def test_metric_with_multiline_expression(self):
        """Metric check with multiline expression (dedented)."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  expression: |
                    COUNT(DISTINCT user_id)
                  threshold:
                    must_be_greater_than: 1000
        """
        check = parse_check_from_contract(yaml_str)
        assert "COUNT(DISTINCT user_id)" in check.expression

    def test_metric_with_filter(self):
        """Metric check with filter."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  expression: "SUM(amount)"
                  filter: "status = 'completed'"
                  threshold:
                    must_be_greater_than: 1000
        """
        check = parse_check_from_contract(yaml_str)
        assert check.expression == "SUM(amount)"
        assert check.filter == "status = 'completed'"

    def test_metric_with_name(self):
        """Metric check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  name: "Total revenue metric"
                  expression: "SUM(revenue)"
                  threshold:
                    must_be_greater_than: 10000
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "Total revenue metric"

    def test_metric_with_threshold_level_warn(self):
        """Metric check with warn threshold level."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  expression: "AVG(score)"
                  threshold:
                    level: warn
                    must_be_greater_than: 3.5
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.level == "warn"

    def test_metric_with_attributes(self):
        """Metric check with custom attributes."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  expression: "COUNT(*)"
                  attributes:
                    owner: "analytics_team"
                    source: "database"
                  threshold:
                    must_be_greater_than: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.attributes == {"owner": "analytics_team", "source": "database"}
