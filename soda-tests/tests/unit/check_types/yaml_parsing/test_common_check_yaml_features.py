"""
Tests for common CheckYaml features shared across all check types.

Tests shared CheckYaml fields (name, filter, attributes, qualifier, etc.)
and ThresholdCheckYaml threshold configurations using parametrization.
"""

import pytest
from helpers.yaml_parsing_helpers import parse_check_from_contract, parse_column_check


class TestCheckNameParsing:
    """Test custom check names."""

    def test_check_name_parsing_row_count(self):
        """Check name is parsed for row_count check."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  name: "At least 100 rows"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name == "At least 100 rows"

    def test_check_name_parsing_column_check(self):
        """Check name is parsed for column-level check."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: user_id
                checks:
                  - missing:
                      name: "No missing user IDs"
        """
        check = parse_column_check(yaml_str)
        assert check.name == "No missing user IDs"

    def test_check_without_name_is_none(self):
        """Check without explicit name has None value."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
        """
        check = parse_check_from_contract(yaml_str)
        assert check.name is None


class TestCheckQualifierParsing:
    """Test check qualifier field."""

    def test_check_qualifier_parsing_string(self):
        """Qualifier is parsed as string."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  qualifier: "production"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.qualifier == "production"

    def test_check_qualifier_parsing_number(self):
        """Qualifier can be numeric and is converted to string."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  qualifier: 123
        """
        check = parse_check_from_contract(yaml_str)
        assert check.qualifier == "123"

    def test_check_without_qualifier_is_none(self):
        """Check without qualifier has None value."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
        """
        check = parse_check_from_contract(yaml_str)
        assert check.qualifier is None


class TestCheckFilterParsing:
    """Test filter expression in checks."""

    def test_check_filter_simple(self):
        """Filter is parsed and stripped."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  filter: "status = 'active'"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.filter == "status = 'active'"

    def test_check_filter_multiline(self):
        """Multiline filter is stripped of leading/trailing whitespace."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  filter: |
                    status = 'active'
                    AND created_at > '2024-01-01'
        """
        check = parse_check_from_contract(yaml_str)
        assert "status = 'active'" in check.filter
        assert "created_at > '2024-01-01'" in check.filter

    def test_check_without_filter_is_none(self):
        """Check without filter has None value."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
        """
        check = parse_check_from_contract(yaml_str)
        assert check.filter is None


class TestCheckAttributesParsing:
    """Test custom attributes in checks."""

    def test_check_attributes_parsing_multiple(self):
        """Custom attributes are parsed as dict."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  attributes:
                    owner: "data_team"
                    priority: "high"
                    env: "prod"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.attributes == {
            "owner": "data_team",
            "priority": "high",
            "env": "prod",
        }

    def test_check_attributes_empty_defaults_to_empty_dict(self):
        """Check without attributes defaults to empty dict."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
        """
        check = parse_check_from_contract(yaml_str)
        assert check.attributes == {}


class TestCheckColumnExpressionParsing:
    """Test column_expression in checks."""

    def test_check_column_expression_simple(self):
        """Column expression is parsed and stripped."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  column_expression: "YEAR(created_at)"
        """
        check = parse_check_from_contract(yaml_str)
        assert check.column_expression == "YEAR(created_at)"

    def test_check_column_expression_with_whitespace(self):
        """Column expression is trimmed."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  column_expression: "  CAST(id AS STRING)  "
        """
        check = parse_check_from_contract(yaml_str)
        assert check.column_expression == "CAST(id AS STRING)"


class TestThresholdMustBeParsing:
    """Test threshold 'must_be' equality check."""

    def test_threshold_must_be_exact_value(self):
        """Threshold must_be matches exact value."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be: 42
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.must_be == 42

    def test_threshold_must_be_float(self):
        """Threshold must_be can be float."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be: 3.14
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.must_be == pytest.approx(3.14)


class TestThresholdMustBeGreaterThanParsing:
    """Test threshold greater-than comparisons."""

    def test_threshold_must_be_greater_than(self):
        """Threshold must_be_greater_than is parsed."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be_greater_than: 100
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.must_be_greater_than == 100

    def test_threshold_must_be_greater_than_or_equal(self):
        """Threshold must_be_greater_than_or_equal is parsed."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be_greater_than_or_equal: 100
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.must_be_greater_than_or_equal == 100


class TestThresholdMustBeLessThanParsing:
    """Test threshold less-than comparisons."""

    def test_threshold_must_be_less_than(self):
        """Threshold must_be_less_than is parsed."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_col
                checks:
                  - missing:
                      threshold:
                        must_be_less_than: 10
        """
        check = parse_column_check(yaml_str)
        assert check.threshold.must_be_less_than == 10

    def test_threshold_must_be_less_than_or_equal(self):
        """Threshold must_be_less_than_or_equal is parsed."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: my_col
                checks:
                  - missing:
                      threshold:
                        must_be_less_than_or_equal: 5
        """
        check = parse_column_check(yaml_str)
        assert check.threshold.must_be_less_than_or_equal == 5


class TestThresholdBetweenParsing:
    """Test threshold between range checks."""

    def test_threshold_must_be_between(self):
        """Threshold must_be_between is parsed with range bounds."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be_between:
                      greater_than_or_equal: 100
                      less_than_or_equal: 1000
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.must_be_between is not None
        assert check.threshold.must_be_between.greater_than_or_equal == 100
        assert check.threshold.must_be_between.less_than_or_equal == 1000

    def test_threshold_must_be_not_between(self):
        """Threshold must_be_not_between is parsed."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be_not_between:
                      greater_than: 0
                      less_than: 100
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.must_be_not_between is not None
        assert check.threshold.must_be_not_between.greater_than == 0
        assert check.threshold.must_be_not_between.less_than == 100


class TestThresholdLevelWarn:
    """Test threshold level field."""

    def test_threshold_level_warn(self):
        """Threshold level can be set to warn."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    level: warn
                    must_be_greater_than: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.level == "warn"

    def test_threshold_level_default_fail(self):
        """Threshold level defaults to fail."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be_greater_than: 0
        """
        check = parse_check_from_contract(yaml_str)
        assert check.threshold.level == "fail"


class TestMissingAndValidityOnColumn:
    """Test missing/validity fields on column-level checks."""

    def test_missing_values_on_column_missing_check(self):
        """missing_values field is parsed in column missing check."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: email
                checks:
                  - missing:
                      missing_values: ["N/A", "null", ""]
        """
        check = parse_column_check(yaml_str)
        assert check.missing_values == ["N/A", "null", ""]

    def test_valid_values_on_column_invalid_check(self):
        """valid_values field is parsed in column invalid check."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: status
                checks:
                  - invalid:
                      valid_values: ["active", "inactive", "pending"]
        """
        check = parse_column_check(yaml_str)
        assert check.valid_values == ["active", "inactive", "pending"]

    def test_valid_min_max_on_column_invalid_check(self):
        """valid_min and valid_max are parsed in column invalid check."""
        yaml_str = """
            dataset: schema/table
            columns:
              - name: age
                checks:
                  - invalid:
                      valid_min: 0
                      valid_max: 150
        """
        check = parse_column_check(yaml_str)
        assert check.valid_min == 0
        assert check.valid_max == 150


class TestStoreFailedRowsFlag:
    """Test store_failed_rows flag."""

    def test_store_failed_rows_true(self):
        """store_failed_rows flag is parsed when true."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - failed_rows:
                  expression: "status = 'error'"
                  store_failed_rows: true
        """
        check = parse_check_from_contract(yaml_str)
        assert check.store_failed_rows is True

    def test_store_failed_rows_false(self):
        """store_failed_rows flag is parsed when false."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
                  store_failed_rows: false
        """
        check = parse_check_from_contract(yaml_str)
        assert check.store_failed_rows is False

    def test_store_failed_rows_defaults_to_false(self):
        """store_failed_rows defaults to false."""
        yaml_str = """
            dataset: schema/table
            columns: []
            checks:
              - row_count:
        """
        check = parse_check_from_contract(yaml_str)
        assert check.store_failed_rows is False
