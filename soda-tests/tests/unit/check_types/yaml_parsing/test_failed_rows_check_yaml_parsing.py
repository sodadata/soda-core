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

    def test_failed_rows_with_keys_query(self):
        """Failed keys form parses keys_query; expression/query None; mapping defaults to identity (None)."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  keys_query: |
                    SELECT customer_id, order_date
                    FROM orders
                    WHERE total < 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.keys_query is not None
        assert "SELECT customer_id, order_date" in check.keys_query
        assert check.expression is None
        assert check.query is None
        assert check.key_column_mapping is None

    def test_failed_rows_keys_query_with_mapping(self):
        """Explicit key_column_mapping parses as a dwh-key -> query-output-column dict."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  keys_query: "SELECT cust_id, order_dt FROM orders WHERE total < 0"
                  key_column_mapping:
                    customer_id: cust_id
                    order_date: order_dt
        """
        check = parse_check_from_contract(yaml_str)
        assert check.key_column_mapping == {"customer_id": "cust_id", "order_date": "order_dt"}

    def test_failed_rows_keys_query_allows_rows_tested_query(self, caplog):
        """rows_tested_query is valid with keys_query (no warning/error)."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  keys_query: "SELECT customer_id FROM orders WHERE total < 0"
                  rows_tested_query: "SELECT count(*) FROM orders"
        """
        with caplog.at_level("WARNING"):
            check = parse_check_from_contract(yaml_str)
        assert check.keys_query is not None
        assert check.rows_tested_query is not None
        assert "only used with" not in caplog.text

    def test_failed_rows_rejects_multiple_forms(self, caplog):
        """query and keys_query together is a mutually-exclusive error."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  query: "SELECT * FROM orders WHERE total < 0"
                  keys_query: "SELECT customer_id FROM orders WHERE total < 0"
        """
        with caplog.at_level("ERROR"):
            parse_check_from_contract(yaml_str)
        assert "mutually exclusive" in caplog.text

    def test_failed_rows_requires_a_form(self, caplog):
        """None of expression/query/keys_query is an error."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  name: missing a form
        """
        with caplog.at_level("ERROR"):
            parse_check_from_contract(yaml_str)
        assert "is required" in caplog.text

    def test_failed_rows_keys_query_mapping_non_string_value_errors(self, caplog):
        """A key_column_mapping whose value is not a string (here an int) is a parse-time error."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  keys_query: "SELECT customer_id FROM orders WHERE total < 0"
                  key_column_mapping:
                    customer_id: 123
        """
        with caplog.at_level("ERROR"):
            parse_check_from_contract(yaml_str)
        assert "must map each key column name" in caplog.text

    def test_failed_rows_keys_query_mapping_nested_value_errors(self, caplog):
        """A key_column_mapping value that is itself an object (not a column-name string) is an error."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  keys_query: "SELECT customer_id FROM orders WHERE total < 0"
                  key_column_mapping:
                    customer_id:
                      nested: cust_id
        """
        with caplog.at_level("ERROR"):
            parse_check_from_contract(yaml_str)
        assert "must map each key column name" in caplog.text

    def test_failed_rows_key_column_mapping_without_keys_query_warns(self, caplog):
        """key_column_mapping on a non-keys_query form (here query) warns it is ignored, but still parses."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  query: "SELECT * FROM orders WHERE total < 0"
                  key_column_mapping:
                    customer_id: cust_id
        """
        with caplog.at_level("WARNING"):
            check = parse_check_from_contract(yaml_str)
        assert check.query is not None
        assert "only used with 'keys_query' mode" in caplog.text

    def test_failed_rows_rejects_all_three_forms(self, caplog):
        """expression, query and keys_query together is a mutually-exclusive error listing all three."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "total < 0"
                  query: "SELECT * FROM orders WHERE total < 0"
                  keys_query: "SELECT customer_id FROM orders WHERE total < 0"
        """
        with caplog.at_level("ERROR"):
            parse_check_from_contract(yaml_str)
        assert "mutually exclusive" in caplog.text
        assert "expression" in caplog.text
        assert "query" in caplog.text
        assert "keys_query" in caplog.text
