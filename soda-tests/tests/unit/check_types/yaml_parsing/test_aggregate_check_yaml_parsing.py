"""
Tests for AggregateCheckYaml parsing.

Focuses on unique fields:
- function: required aggregation function name
- Extends MissingAncValidityCheckYaml (inherits metric, unit, threshold)
- Dataset-level only
"""


from helpers.yaml_parsing_helpers import parse_check_from_contract


class TestAggregateCheckYamlParsing:
    def test_aggregate_with_function_avg(self):
        """Aggregate check with AVG function."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  function: AVG
                  column: amount
                  threshold:
                    must_be_greater_than: 100
        """
        check = parse_check_from_contract(yaml_str)
        assert check.type_name == "aggregate"
        assert check.function == "AVG"

    def test_aggregate_with_function_sum(self):
        """Aggregate check with SUM function."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  function: SUM
                  column: revenue
                  threshold:
                    must_be_greater_than: 10000
        """
        check = parse_check_from_contract(yaml_str)
        assert check.function == "SUM"

    def test_aggregate_with_function_max(self):
        """Aggregate check with MAX function."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  function: MAX
                  column: price
                  threshold:
                    must_be_less_than: 1000
        """
        check = parse_check_from_contract(yaml_str)
        assert check.function == "MAX"

    def test_aggregate_with_function_min(self):
        """Aggregate check with MIN function."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  function: MIN
                  column: score
                  threshold:
                    must_be_greater_than_or_equal: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.function == "MIN"

    def test_aggregate_with_filter(self):
        """Aggregate check with filter condition."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  function: AVG
                  column: rating
                  filter: "status = 'active'"
                  threshold:
                    must_be_greater_than: 4.0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.function == "AVG"
        assert check.filter == "status = 'active'"

    def test_aggregate_with_name(self):
        """Aggregate check with custom name."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  name: "Average order value"
                  function: AVG
                  column: order_total
                  threshold:
                    must_be_greater_than: 50
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "Average order value"

    def test_aggregate_with_threshold_level_warn(self):
        """Aggregate check with warn threshold level."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - aggregate:
                  function: COUNT
                  column: id
                  threshold:
                    level: warn
                    must_be_greater_than: 100
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.level == "warn"
