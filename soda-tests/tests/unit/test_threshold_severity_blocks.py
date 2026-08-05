import pytest
from helpers.test_functions import dedent_and_strip
from soda_core.common.yaml import ContractYamlSource, YamlObject
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
