"""
Tests for FailedRowsCheckYaml parsing.

Focuses on unique fields:
- expression: optional condition expression
- query: optional custom SQL query (multiline supported)
- Either expression or query is required (not both optional)
- Extends ThresholdCheckYaml (inherits metric, unit, threshold)
- Dataset-level only

Common fields (name, filter, threshold, metric, store_failed_rows) are tested in test_common_check_yaml_features.py.
"""

from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestFailedRowsCheckYamlParsing:
    def test_failed_rows_with_expression(self):
        """Failed rows check parses expression, query is None."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "age < 0"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "failed_rows"
        assert check.expression == "age < 0"
        assert check.query is None
        assert check.name is None
        assert check.filter is None
        assert check.threshold is None

    def test_failed_rows_with_multiline_query(self):
        """Failed rows check parses multiline query, expression is None."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM my_dataset
                    WHERE status NOT IN ('active', 'pending')
        """
        check = parse_check_from_contract(yaml_str)
        assert check.query is not None
        assert "status NOT IN" in check.query
        assert "FROM my_dataset" in check.query
        assert check.expression is None
