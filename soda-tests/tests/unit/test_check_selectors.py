from unittest.mock import MagicMock

import pytest
from soda_core.contracts.impl.check_selector import (
    CheckSelector,
    CheckSelectorParseException,
)


def _make_check_impl(
    type="missing",
    name="No missing values",
    column_name="id",
    path="columns.id.checks.missing",
    qualifier=None,
    attributes=None,
):
    """Create a mock CheckImpl with the given attributes."""
    check_impl = MagicMock()
    check_impl.type = type
    check_impl.name = name
    check_impl.path = path
    check_impl.attributes = attributes or {}

    if column_name:
        check_impl.column_impl.column_yaml.name = column_name
    else:
        check_impl.column_impl = None

    check_yaml = MagicMock()
    check_yaml.qualifier = qualifier
    check_impl.check_yaml = check_yaml

    return check_impl


# --- Parsing tests ---


class TestCheckSelectorParse:
    def test_parse_simple(self):
        selector = CheckSelector.parse("type=missing")
        assert selector.field == "type"
        assert selector.value == "missing"

    def test_parse_with_spaces(self):
        selector = CheckSelector.parse(" type = missing ")
        assert selector.field == "type"
        assert selector.value == "missing"

    def test_parse_attribute_dot_notation(self):
        selector = CheckSelector.parse("attributes.severity=critical")
        assert selector.field == "attributes.severity"
        assert selector.value == "critical"

    def test_parse_value_with_equals(self):
        selector = CheckSelector.parse("name=a=b")
        assert selector.field == "name"
        assert selector.value == "a=b"

    def test_parse_empty_value(self):
        selector = CheckSelector.parse("type=")
        assert selector.field == "type"
        assert selector.value == ""

    def test_parse_no_equals_raises(self):
        with pytest.raises(CheckSelectorParseException):
            CheckSelector.parse("type_missing")

    def test_parse_empty_field_raises(self):
        with pytest.raises(CheckSelectorParseException):
            CheckSelector.parse("=value")

    def test_parse_unknown_field_raises(self):
        with pytest.raises(CheckSelectorParseException):
            CheckSelector.parse("unknown_field=value")

    def test_parse_all_valid(self):
        selectors = CheckSelector.parse_all(["type=missing", "column=id"])
        assert len(selectors) == 2

    def test_parse_all_with_error(self):
        with pytest.raises(CheckSelectorParseException):
            CheckSelector.parse_all(["type=missing", "bad_expression"])

    def test_parse_all_empty(self):
        selectors = CheckSelector.parse_all([])
        assert selectors == []

    def test_parse_all_none(self):
        selectors = CheckSelector.parse_all(None)
        assert selectors == []

    def test_parse_all_supported_fields(self):
        for field in ["type", "name", "column", "path", "qualifier"]:
            selector = CheckSelector.parse(f"{field}=value")
            assert selector.field == field

    def test_from_check_paths(self):
        selectors = CheckSelector.from_check_paths(["columns.id.checks.missing", "checks.schema"])
        assert len(selectors) == 2
        assert selectors[0].field == "path"
        assert selectors[0].value == "columns.id.checks.missing"
        assert selectors[1].field == "path"
        assert selectors[1].value == "checks.schema"

    def test_from_check_paths_none(self):
        selectors = CheckSelector.from_check_paths(None)
        assert selectors == []

    def test_from_check_paths_empty(self):
        selectors = CheckSelector.from_check_paths([])
        assert selectors == []


# --- Matching tests ---


class TestCheckSelectorMatches:
    def test_match_type(self):
        selector = CheckSelector.parse("type=missing")
        check = _make_check_impl(type="missing")
        assert selector.matches(check)

    def test_no_match_type(self):
        selector = CheckSelector.parse("type=invalid")
        check = _make_check_impl(type="missing")
        assert not selector.matches(check)

    def test_match_name(self):
        selector = CheckSelector.parse("name=No missing values")
        check = _make_check_impl(name="No missing values")
        assert selector.matches(check)

    def test_match_column(self):
        selector = CheckSelector.parse("column=id")
        check = _make_check_impl(column_name="id")
        assert selector.matches(check)

    def test_no_match_column_none(self):
        selector = CheckSelector.parse("column=id")
        check = _make_check_impl(column_name=None)
        assert not selector.matches(check)

    def test_match_path(self):
        selector = CheckSelector.parse("path=columns.id.checks.missing")
        check = _make_check_impl(path="columns.id.checks.missing")
        assert selector.matches(check)

    def test_match_qualifier(self):
        selector = CheckSelector.parse("qualifier=important")
        check = _make_check_impl(qualifier="important")
        assert selector.matches(check)

    def test_no_match_qualifier_none(self):
        selector = CheckSelector.parse("qualifier=important")
        check = _make_check_impl(qualifier=None)
        assert not selector.matches(check)

    def test_match_attribute(self):
        selector = CheckSelector.parse("attributes.severity=critical")
        check = _make_check_impl(attributes={"severity": "critical"})
        assert selector.matches(check)

    def test_no_match_attribute_missing_key(self):
        selector = CheckSelector.parse("attributes.severity=critical")
        check = _make_check_impl(attributes={})
        assert not selector.matches(check)

    def test_match_attribute_numeric_value(self):
        selector = CheckSelector.parse("attributes.priority=1")
        check = _make_check_impl(attributes={"priority": 1})
        assert selector.matches(check)


# --- Wildcard tests ---


class TestCheckSelectorWildcard:
    def test_wildcard_star(self):
        selector = CheckSelector.parse("type=miss*")
        check = _make_check_impl(type="missing")
        assert selector.matches(check)

    def test_wildcard_question(self):
        selector = CheckSelector.parse("type=missin?")
        check = _make_check_impl(type="missing")
        assert selector.matches(check)

    def test_wildcard_no_match(self):
        selector = CheckSelector.parse("type=inv*")
        check = _make_check_impl(type="missing")
        assert not selector.matches(check)

    def test_wildcard_column_prefix(self):
        selector = CheckSelector.parse("column=user_*")
        check = _make_check_impl(column_name="user_id")
        assert selector.matches(check)

    def test_wildcard_path(self):
        selector = CheckSelector.parse("path=columns.*.checks.missing")
        check = _make_check_impl(path="columns.id.checks.missing")
        assert selector.matches(check)

    def test_wildcard_with_brackets_treated_literally(self):
        selector = CheckSelector.parse("name=test[1]*")
        check_match = _make_check_impl(name="test[1]foo")
        check_no_match = _make_check_impl(name="test1foo")
        assert selector.matches(check_match)
        assert not selector.matches(check_no_match)


# --- Grouping / all_match tests ---


class TestCheckSelectorAllMatch:
    def test_empty_selectors_always_match(self):
        check = _make_check_impl()
        assert CheckSelector.all_match([], check)

    def test_single_selector_match(self):
        selectors = CheckSelector.parse_all(["type=missing"])
        check = _make_check_impl(type="missing")
        assert CheckSelector.all_match(selectors, check)

    def test_single_selector_no_match(self):
        selectors = CheckSelector.parse_all(["type=invalid"])
        check = _make_check_impl(type="missing")
        assert not CheckSelector.all_match(selectors, check)

    def test_and_across_different_fields(self):
        selectors = CheckSelector.parse_all(["type=missing", "column=id"])
        check = _make_check_impl(type="missing", column_name="id")
        assert CheckSelector.all_match(selectors, check)

    def test_and_across_different_fields_no_match(self):
        selectors = CheckSelector.parse_all(["type=missing", "column=name"])
        check = _make_check_impl(type="missing", column_name="id")
        assert not CheckSelector.all_match(selectors, check)

    def test_or_within_same_field(self):
        selectors = CheckSelector.parse_all(["type=missing", "type=invalid"])
        check_missing = _make_check_impl(type="missing")
        check_invalid = _make_check_impl(type="invalid")
        check_schema = _make_check_impl(type="schema")
        assert CheckSelector.all_match(selectors, check_missing)
        assert CheckSelector.all_match(selectors, check_invalid)
        assert not CheckSelector.all_match(selectors, check_schema)

    def test_combined_or_and_and(self):
        # (type=missing OR type=invalid) AND column=id
        selectors = CheckSelector.parse_all(["type=missing", "type=invalid", "column=id"])
        check1 = _make_check_impl(type="missing", column_name="id")
        check2 = _make_check_impl(type="invalid", column_name="id")
        check3 = _make_check_impl(type="missing", column_name="name")
        check4 = _make_check_impl(type="schema", column_name="id")
        assert CheckSelector.all_match(selectors, check1)
        assert CheckSelector.all_match(selectors, check2)
        assert not CheckSelector.all_match(selectors, check3)
        assert not CheckSelector.all_match(selectors, check4)

    def test_check_paths_converted_to_selectors(self):
        selectors = CheckSelector.from_check_paths(["columns.id.checks.missing", "checks.schema"])
        check1 = _make_check_impl(path="columns.id.checks.missing")
        check2 = _make_check_impl(path="checks.schema")
        check3 = _make_check_impl(path="columns.name.checks.invalid")
        assert CheckSelector.all_match(selectors, check1)
        assert CheckSelector.all_match(selectors, check2)
        assert not CheckSelector.all_match(selectors, check3)

    def test_check_paths_combined_with_selectors(self):
        path_selectors = CheckSelector.from_check_paths(["columns.id.checks.missing"])
        attr_selectors = CheckSelector.parse_all(["attributes.severity=critical"])
        all_selectors = path_selectors + attr_selectors

        check_match = _make_check_impl(path="columns.id.checks.missing", attributes={"severity": "critical"})
        check_wrong_path = _make_check_impl(path="checks.schema", attributes={"severity": "critical"})
        check_wrong_attr = _make_check_impl(path="columns.id.checks.missing", attributes={"severity": "low"})

        assert CheckSelector.all_match(all_selectors, check_match)
        assert not CheckSelector.all_match(all_selectors, check_wrong_path)
        assert not CheckSelector.all_match(all_selectors, check_wrong_attr)


# --- List attribute tests ---


class TestCheckSelectorParseListValue:
    def test_valid(self):
        assert CheckSelector._parse_list_value("[a,b,c]") == ["a", "b", "c"]

    def test_with_spaces(self):
        assert CheckSelector._parse_list_value("[a, b , c]") == ["a", "b", "c"]

    def test_empty(self):
        assert CheckSelector._parse_list_value("[]") == []

    def test_not_list(self):
        assert CheckSelector._parse_list_value("prod") is None

    def test_partial_bracket(self):
        assert CheckSelector._parse_list_value("[prod") is None

    def test_single_element(self):
        assert CheckSelector._parse_list_value("[prod]") == ["prod"]

    def test_quoted_element_with_comma(self):
        assert CheckSelector._parse_list_value('["a,b",c]') == ["a,b", "c"]

    def test_quoted_element_with_spaces(self):
        assert CheckSelector._parse_list_value('["hello world", foo]') == ["hello world", "foo"]

    def test_all_quoted(self):
        assert CheckSelector._parse_list_value('["a,b","c,d"]') == ["a,b", "c,d"]

    def test_mixed_quoted_unquoted(self):
        assert CheckSelector._parse_list_value('[a, "b,c", d]') == ["a", "b,c", "d"]

    def test_unterminated_quote_raises(self):
        with pytest.raises(CheckSelectorParseException):
            CheckSelector._parse_list_value('["a,b]')

    def test_unterminated_quote_middle_raises(self):
        with pytest.raises(CheckSelectorParseException):
            CheckSelector._parse_list_value('[a, "b, c]')


class TestCheckSelectorListMemberMatch:
    def test_member_match(self):
        selector = CheckSelector.parse("attributes.tags=prod")
        check = _make_check_impl(attributes={"tags": ["prod", "critical"]})
        assert selector.matches(check)

    def test_member_no_match(self):
        selector = CheckSelector.parse("attributes.tags=staging")
        check = _make_check_impl(attributes={"tags": ["prod", "critical"]})
        assert not selector.matches(check)

    def test_member_match_wildcard(self):
        selector = CheckSelector.parse("attributes.tags=prod*")
        check = _make_check_impl(attributes={"tags": ["prod-us", "prod-eu"]})
        assert selector.matches(check)

    def test_member_match_numeric(self):
        selector = CheckSelector.parse("attributes.priority=2")
        check = _make_check_impl(attributes={"priority": [1, 2, 3]})
        assert selector.matches(check)

    def test_member_match_empty_list(self):
        selector = CheckSelector.parse("attributes.tags=prod")
        check = _make_check_impl(attributes={"tags": []})
        assert not selector.matches(check)


class TestCheckSelectorListFullMatch:
    def test_full_match(self):
        selector = CheckSelector.parse("attributes.tags=[prod,critical]")
        check = _make_check_impl(attributes={"tags": ["prod", "critical"]})
        assert selector.matches(check)

    def test_full_match_order_independent(self):
        selector = CheckSelector.parse("attributes.tags=[critical,prod]")
        check = _make_check_impl(attributes={"tags": ["prod", "critical"]})
        assert selector.matches(check)

    def test_full_match_wrong_cardinality(self):
        selector = CheckSelector.parse("attributes.tags=[prod]")
        check = _make_check_impl(attributes={"tags": ["prod", "critical"]})
        assert not selector.matches(check)

    def test_full_match_extra_element(self):
        selector = CheckSelector.parse("attributes.tags=[prod,critical]")
        check = _make_check_impl(attributes={"tags": ["prod"]})
        assert not selector.matches(check)

    def test_full_match_empty(self):
        selector = CheckSelector.parse("attributes.tags=[]")
        check = _make_check_impl(attributes={"tags": []})
        assert selector.matches(check)

    def test_full_match_no_wildcards(self):
        selector = CheckSelector.parse("attributes.tags=[prod*]")
        check = _make_check_impl(attributes={"tags": ["prod-us"]})
        assert not selector.matches(check)


class TestCheckSelectorListAllMatch:
    def test_or_within_same_list_attribute(self):
        selectors = CheckSelector.parse_all(["attributes.tags=prod", "attributes.tags=staging"])
        check_prod = _make_check_impl(attributes={"tags": ["prod", "critical"]})
        check_staging = _make_check_impl(attributes={"tags": ["staging"]})
        check_dev = _make_check_impl(attributes={"tags": ["dev"]})
        assert CheckSelector.all_match(selectors, check_prod)
        assert CheckSelector.all_match(selectors, check_staging)
        assert not CheckSelector.all_match(selectors, check_dev)

    def test_and_across_list_attribute_and_type(self):
        selectors = CheckSelector.parse_all(["attributes.tags=prod", "type=missing"])
        check_match = _make_check_impl(type="missing", attributes={"tags": ["prod", "critical"]})
        check_wrong_type = _make_check_impl(type="invalid", attributes={"tags": ["prod"]})
        check_wrong_attr = _make_check_impl(type="missing", attributes={"tags": ["staging"]})
        assert CheckSelector.all_match(selectors, check_match)
        assert not CheckSelector.all_match(selectors, check_wrong_type)
        assert not CheckSelector.all_match(selectors, check_wrong_attr)
