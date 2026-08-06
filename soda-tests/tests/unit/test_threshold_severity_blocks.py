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


def test_severity_blocks_parsed():
    threshold_yaml = parse_threshold_yaml(
        """
        fail:
          must_be_less_than: 10
        warn:
          must_be_less_than: 5
        """
    )
    assert threshold_yaml.has_severity_blocks()
    assert threshold_yaml.fail_block.must_be_less_than == 10
    assert threshold_yaml.warn_block.must_be_less_than == 5


def test_fail_only_block_parsed():
    threshold_yaml = parse_threshold_yaml("fail:\n  must_be_greater_than: 0")
    assert threshold_yaml.fail_block.must_be_greater_than == 0
    assert threshold_yaml.warn_block is None


def test_warn_block_with_range_parsed():
    threshold_yaml = parse_threshold_yaml(
        """
        fail:
          must_be_between:
            greater_than: 0
            less_than: 100
        warn:
          must_be_between:
            greater_than: 10
            less_than: 90
        """
    )
    assert threshold_yaml.warn_block.must_be_between.greater_than == 10


def test_flat_and_blocks_mix_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nwarn:\n  must_be_less_than: 5")
    assert "cannot be combined" in caplog.text


def test_level_and_blocks_mix_is_error(caplog):
    parse_threshold_yaml("level: warn\nfail:\n  must_be_less_than: 10")
    assert "'level' cannot be combined" in caplog.text


def test_unknown_key_in_block_is_error(caplog):
    parse_threshold_yaml("fail:\n  must_be_less_then: 10")
    assert "not allowed in a severity block" in caplog.text


def test_metric_in_block_is_error(caplog):
    parse_threshold_yaml("fail:\n  metric: percent\n  must_be_less_than: 10")
    assert "not allowed in a severity block" in caplog.text


def test_block_without_comparison_is_error(caplog):
    parse_threshold_yaml("fail: {}\nwarn:\n  must_be_less_than: 5")
    assert "exactly one comparison" in caplog.text


def test_block_with_two_comparisons_is_error(caplog):
    parse_threshold_yaml("warn:\n  must_be_less_than: 5\n  must_be_greater_than: 1")
    assert "exactly one comparison" in caplog.text


def test_null_severity_block_is_error(caplog):
    threshold_yaml = parse_threshold_yaml("must_be_less_than: 10\nwarn:")
    assert threshold_yaml.warn_block is None
    assert "'warn' severity block is empty" in caplog.text


def create_impl(threshold_yaml_str: str) -> ThresholdImpl:
    return ThresholdImpl.create(threshold_yaml=parse_threshold_yaml(threshold_yaml_str))


def test_fail_block_creates_fail_level_impl():
    impl = create_impl("fail:\n  must_be_less_than: 10")
    assert impl.level == ThresholdLevel.FAIL
    assert impl.passes(9) and not impl.passes(10)


def test_warn_only_block_creates_warn_level_impl():
    impl = create_impl("warn:\n  must_be_greater_than: 100")
    assert impl.level == ThresholdLevel.WARN
    assert impl.passes(101) and not impl.passes(100)


def test_warn_only_block_ignores_default_threshold():
    from soda_core.contracts.impl.contract_verification_impl import ThresholdType

    default = ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be_greater_than=0)
    impl = ThresholdImpl.create(
        threshold_yaml=parse_threshold_yaml("warn:\n  must_be_greater_than: 100"),
        default_threshold=default,
    )
    assert impl.level == ThresholdLevel.WARN
    assert impl.must_be_greater_than == 100


def test_create_from_comparisons_builds_warn_impl_from_warn_block():
    threshold_yaml = parse_threshold_yaml("fail:\n  must_be_less_than: 10\nwarn:\n  must_be_less_than: 5")
    warn_impl = ThresholdImpl.create_from_comparisons(threshold_yaml.warn_block, ThresholdLevel.WARN)
    assert warn_impl.level == ThresholdLevel.WARN
    assert warn_impl.passes(4) and not warn_impl.passes(5)


def test_warn_tighter_than_fail_can_fire():
    fail_impl = create_impl("fail:\n  must_be_less_than: 10")
    warn_impl = ThresholdImpl.create_from_comparisons(parse_threshold_yaml("must_be_less_than: 5"), ThresholdLevel.WARN)
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_warn_wider_than_fail_is_dead():
    fail_impl = create_impl("fail:\n  must_be_less_than: 5")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_less_than: 10"), ThresholdLevel.WARN
    )
    assert not warn_can_fire_alone(fail_impl, warn_impl)


def test_warn_equal_to_fail_is_dead():
    fail_impl = create_impl("fail:\n  must_be_less_than: 10")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_less_than: 10"), ThresholdLevel.WARN
    )
    assert not warn_can_fire_alone(fail_impl, warn_impl)


def test_nested_ranges_can_fire():
    fail_impl = create_impl("fail:\n  must_be_between:\n    greater_than: 0\n    less_than: 100")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_between:\n  greater_than: 10\n  less_than: 90"), ThresholdLevel.WARN
    )
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_outer_range_warn_can_fire():
    # warn passes outside [0,100] (not_between 0..100); fail passes outside [10,90].
    # fail's pass region is NOT inside warn's pass region -> warn can fire (value 5).
    fail_impl = create_impl("fail:\n  must_be_not_between:\n    less_than_or_equal: 10\n    greater_than_or_equal: 90")
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
                    fail:
                      must_be_greater_than: 100
                    warn:
                      must_be_greater_than: 10
            """
        )
    assert "can never produce a warn outcome" in caplog.text
