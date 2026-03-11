"""
Tests for FailedRowsCheckYaml parsing.

Focuses on unique fields:
- expression: optional condition expression
- query: optional custom SQL query
- Either expression or query is required (not both optional)
- Extends ThresholdCheckYaml (inherits metric, unit, threshold)
- Dataset-level only
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestFailedRowsCheckYamlParsing:
    def test_failed_rows_with_expression(self):
        """Failed rows check with expression."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "age < 0"
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "failed_rows"
        assert check.expression == "age < 0"
        assert check.query is None

    def test_failed_rows_with_query(self):
        """Failed rows check with query."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  query: |
                    SELECT * FROM my_dataset
                    WHERE amount < 0
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.query is not None
        assert "amount < 0" in check.query
        assert check.expression is None

    def test_failed_rows_with_complex_expression(self):
        """Failed rows check with complex expression."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "(price < 0) OR (quantity > 1000)"
                  threshold:
                    must_be_less_than: 5
        """
        check = parse_check_from_contract(yaml_str)
        assert "(price < 0)" in check.expression
        assert "(quantity > 1000)" in check.expression

    def test_failed_rows_with_multiline_query(self):
        """Failed rows check with multiline query (dedented)."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  query: |
                    SELECT *
                    FROM my_dataset
                    WHERE status NOT IN ('active', 'pending', 'completed')
                  threshold:
                    must_be_less_than_or_equal: 1
        """
        check = parse_check_from_contract(yaml_str)
        assert "status NOT IN" in check.query

    def test_failed_rows_with_filter(self):
        """Failed rows check with filter."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "value < 0"
                  filter: "type = 'numeric'"
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.expression == "value < 0"
        assert check.filter == "type = 'numeric'"

    def test_failed_rows_with_name(self):
        """Failed rows check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  name: "Check for negative amounts"
                  expression: "amount < 0"
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "Check for negative amounts"

    def test_failed_rows_with_metric_percent(self):
        """Failed rows check with percent metric."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "email NOT LIKE '%@%'"
                  threshold:
                    metric: percent
                    must_be_less_than: 1.0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.metric == "percent"

    def test_failed_rows_with_store_failed_rows(self):
        """Failed rows check with store_failed_rows flag."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "age < 0"
                  store_failed_rows: true
                  threshold:
                    must_be_less_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.store_failed_rows is True
