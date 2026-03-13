"""
Tests for MetricCheckYaml parsing.

Focuses on unique fields:
- expression: optional custom metric expression
- query: optional custom SQL query (multiline supported)
- Either expression or query is required (not both optional)
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Dataset-level only

Common fields (name, filter, threshold, metric, attributes) are tested in test_common_check_yaml_features.py.
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestMetricCheckYamlParsing:
    def test_metric_with_expression(self):
        """Metric check parses expression, query is None."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  expression: "COUNT(*)"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "metric"
        assert check.expression == "COUNT(*)"
        assert check.query is None
        assert check.name is None
        assert check.filter is None
        assert check.threshold is None

    def test_metric_with_multiline_query(self):
        """Metric check parses multiline query, expression is None."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - metric:
                  query: |
                    SELECT COUNT(*) as value
                    FROM my_dataset
                    WHERE status = 'active'
        """
        check = parse_check_from_contract(yaml_str)
        assert check.query is not None
        assert "COUNT(*)" in check.query
        assert "FROM my_dataset" in check.query
        assert check.expression is None
