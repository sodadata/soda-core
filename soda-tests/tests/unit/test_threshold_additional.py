import logging

from helpers.impl_test_helpers import build_contract_impl
from helpers.test_functions import dedent_and_strip
from soda_core.common.yaml import ContractYamlSource, YamlObject
from soda_core.contracts.impl.contract_verification_impl import (
    ThresholdImpl,
    ThresholdLevel,
    warn_can_fire_alone,
)
from soda_core.contracts.impl.contract_yaml import ThresholdYaml


def parse_threshold_yaml(threshold_yaml_str: str) -> ThresholdYaml:
    yaml_source = ContractYamlSource.from_str(dedent_and_strip(threshold_yaml_str))
    yaml_object: YamlObject = yaml_source.parse()
    return ThresholdYaml(threshold_yaml_object=yaml_object)


def assert_no_errors(caplog) -> None:
    errors: list[str] = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == []


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_additional_threshold_parsed():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 1
        additional:
          must_be_less_than: 0
          level: warn
        """
    )
    assert threshold_yaml.must_be_less_than == 1
    assert threshold_yaml.level == "fail"
    assert threshold_yaml.additional.must_be_less_than == 0
    assert threshold_yaml.additional.level == "warn"


def test_additional_absent_is_none():
    threshold_yaml = parse_threshold_yaml("must_be_less_than: 1")
    assert threshold_yaml.additional is None
    assert threshold_yaml.level == "fail"


def test_additional_level_defaults_to_fail():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
        """
    )
    assert threshold_yaml.level == "warn"
    assert threshold_yaml.additional.level == "fail"


def test_additional_with_range_parsed():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_between:
          greater_than: 0
          less_than: 100
        additional:
          must_be_between:
            greater_than: 10
            less_than: 90
          level: warn
        """
    )
    assert threshold_yaml.additional.must_be_between.greater_than == 10
    assert threshold_yaml.additional.must_be_between.less_than == 90


# ---------------------------------------------------------------------------
# Validations
# ---------------------------------------------------------------------------


def test_outer_warn_with_bare_additional_is_valid(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
        """
    )
    assert_no_errors(caplog)


def test_outer_fail_with_warn_additional_is_valid(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 1
        additional:
          must_be_less_than: 10
          level: warn
        """
    )
    assert_no_errors(caplog)


def test_same_level_by_default_is_error(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 1
        """
    )
    assert "level 'fail'" in caplog.text
    assert "'additional' threshold level 'fail'" in caplog.text
    assert "must be different" in caplog.text


def test_explicit_fail_fail_is_error(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: fail
        additional:
          must_be_less_than: 1
          level: fail
        """
    )
    assert "must be different" in caplog.text


def test_warn_warn_is_error(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
          level: warn
        """
    )
    assert "level 'warn'" in caplog.text
    assert "'additional' threshold level 'warn'" in caplog.text
    assert "must be different" in caplog.text


def test_additional_without_outer_comparison_is_error(caplog):
    parse_threshold_yaml(
        """
        additional:
          must_be_less_than: 1
          level: warn
        """
    )
    assert "must specify a comparison" in caplog.text


def test_unknown_key_in_additional_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  must_be_less_then: 1")
    assert "not allowed in an 'additional' threshold" in caplog.text


def test_metric_in_additional_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  metric: percent\n  must_be_less_than: 1")
    assert "not allowed in an 'additional' threshold" in caplog.text


def test_unit_in_additional_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  unit: day\n  must_be_less_than: 1")
    assert "not allowed in an 'additional' threshold" in caplog.text


def test_nested_additional_is_error(caplog):
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 5
          level: warn
          additional:
            must_be_less_than: 1
        """
    )
    assert "not allowed in an 'additional' threshold" in caplog.text
    assert threshold_yaml.additional.additional is None


def test_additional_without_comparison_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional: {}")
    assert "exactly one comparison" in caplog.text


def test_additional_with_two_comparisons_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  must_be_less_than: 5\n  must_be_greater_than: 1")
    assert "exactly one comparison" in caplog.text


def test_null_additional_is_error(caplog):
    threshold_yaml = parse_threshold_yaml("must_be_less_than: 10\nadditional:")
    assert threshold_yaml.additional is None
    assert "'additional' threshold is empty" in caplog.text


# ---------------------------------------------------------------------------
# Impl assignment by level
# ---------------------------------------------------------------------------


def create_impl(threshold_yaml_str: str) -> ThresholdImpl:
    return ThresholdImpl.create(threshold_yaml=parse_threshold_yaml(threshold_yaml_str))


def test_flat_threshold_unchanged():
    impl = create_impl("must_be_less_than: 10")
    assert impl.level == ThresholdLevel.FAIL
    assert impl.passes(9) and not impl.passes(10)


def test_flat_warn_level_threshold_unchanged():
    impl = create_impl("must_be_less_than: 10\nlevel: warn")
    assert impl.level == ThresholdLevel.WARN
    assert impl.passes(9) and not impl.passes(10)


def test_outer_fail_additional_warn_keeps_outer_as_primary():
    impl = create_impl(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 1
          level: warn
        """
    )
    assert impl.level == ThresholdLevel.FAIL
    assert impl.must_be_less_than == 10


def test_outer_warn_additional_fail_makes_additional_the_primary():
    impl = create_impl(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
        """
    )
    assert impl.level == ThresholdLevel.FAIL
    assert impl.must_be_less_than == 1


def test_additional_ignores_default_threshold():
    from soda_core.contracts.impl.contract_verification_impl import ThresholdType

    default = ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be_greater_than=0)
    impl = ThresholdImpl.create(
        threshold_yaml=parse_threshold_yaml(
            """
            must_be_greater_than: 100
            additional:
              must_be_greater_than: 10
              level: warn
            """
        ),
        default_threshold=default,
    )
    assert impl is not default
    assert impl.level == ThresholdLevel.FAIL
    assert impl.must_be_greater_than == 100


def test_create_from_comparisons_builds_warn_impl_from_additional():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 5
          level: warn
        """
    )
    warn_impl = ThresholdImpl.create_from_comparisons(threshold_yaml.additional, ThresholdLevel.WARN)
    assert warn_impl.level == ThresholdLevel.WARN
    assert warn_impl.passes(4) and not warn_impl.passes(5)


# ---------------------------------------------------------------------------
# Dead-warn lint (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_warn_tighter_than_fail_can_fire():
    fail_impl = create_impl("must_be_less_than: 10")
    warn_impl = ThresholdImpl.create_from_comparisons(parse_threshold_yaml("must_be_less_than: 5"), ThresholdLevel.WARN)
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_warn_wider_than_fail_is_dead():
    fail_impl = create_impl("must_be_less_than: 5")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_less_than: 10"), ThresholdLevel.WARN
    )
    assert not warn_can_fire_alone(fail_impl, warn_impl)


def test_warn_equal_to_fail_is_dead():
    fail_impl = create_impl("must_be_less_than: 10")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_less_than: 10"), ThresholdLevel.WARN
    )
    assert not warn_can_fire_alone(fail_impl, warn_impl)


def test_nested_ranges_can_fire():
    fail_impl = create_impl("must_be_between:\n  greater_than: 0\n  less_than: 100")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_between:\n  greater_than: 10\n  less_than: 90"), ThresholdLevel.WARN
    )
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_outer_range_warn_can_fire():
    # warn passes outside [0,100] (not_between 0..100); fail passes outside [10,90].
    # fail's pass region is NOT inside warn's pass region -> warn can fire (value 5).
    fail_impl = create_impl("must_be_not_between:\n  less_than_or_equal: 10\n  greater_than_or_equal: 90")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_not_between:\n  less_than_or_equal: 0\n  greater_than_or_equal: 100"),
        ThresholdLevel.WARN,
    )
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_dead_warn_logs_lint_warning(caplog):
    with caplog.at_level(logging.WARNING):
        build_contract_impl(
            """
            dataset: my_data_source/my_dataset
            columns:
              - name: id
                data_type: integer
            checks:
              - row_count:
                  threshold:
                    must_be_greater_than: 100
                    additional:
                      must_be_greater_than: 10
                      level: warn
            """
        )
    assert "can never produce a warn outcome" in caplog.text
