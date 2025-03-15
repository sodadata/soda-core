from soda_core.common.yaml import YamlObject, YamlSource
from soda_core.contracts.impl.contract_verification_impl import ThresholdImpl
from soda_core.contracts.impl.contract_yaml import ThresholdYaml
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_threshold_must_be():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be: 0
    """
    )

    assert threshold.get_assertion_summary("m") == "m = 0"
    assert not threshold.passes(-1)
    assert threshold.passes(0)
    assert not threshold.passes(1)


def test_threshold_must_not_be():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_not_be: 0
    """
    )

    assert threshold.get_assertion_summary("m") == "m != 0"
    assert threshold.passes(-1)
    assert not threshold.passes(0)
    assert threshold.passes(1)


def test_threshold_greater_than():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_greater_than: 0
    """
    )

    assert threshold.get_assertion_summary("m") == "0 < m"
    assert not threshold.passes(-1)
    assert not threshold.passes(0)
    assert threshold.passes(1)


def test_threshold_greater_than_or_equal():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_greater_than_or_equal: 0
    """
    )

    assert threshold.get_assertion_summary("m") == "0 <= m"
    assert not threshold.passes(-1)
    assert threshold.passes(0)
    assert threshold.passes(1)


def test_threshold_less_than():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_less_than: 0
    """
    )

    assert threshold.get_assertion_summary("m") == "m < 0"
    assert threshold.passes(-1)
    assert not threshold.passes(0)
    assert not threshold.passes(1)


def test_threshold_less_than_or_equal():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_less_than_or_equal: 0
    """
    )

    assert threshold.get_assertion_summary("m") == "m <= 0"
    assert threshold.passes(-1)
    assert threshold.passes(0)
    assert not threshold.passes(1)


def test_threshold_between():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_between: [0, 1]
    """
    )

    assert threshold.get_assertion_summary("m") == "0 <= m <= 1"
    assert not threshold.passes(-1)
    assert threshold.passes(0)
    assert threshold.passes(1)
    assert not threshold.passes(2)


def test_threshold_not_between():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_not_between: [0, 2]
    """
    )

    assert threshold.get_assertion_summary("m") == "m <= 0 or 2 <= m"
    assert threshold.passes(-1)
    assert threshold.passes(0)
    assert not threshold.passes(1)
    assert threshold.passes(2)
    assert threshold.passes(3)


def test_threshold_custom_inner():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_greater_than: 0
        must_be_less_than_or_equal: 2
    """
    )

    assert threshold.get_assertion_summary("m") == "0 < m <= 2"
    assert not threshold.passes(-1)
    assert not threshold.passes(0)
    assert threshold.passes(1)
    assert threshold.passes(2)
    assert not threshold.passes(3)


def test_threshold_custom_outer():
    threshold: ThresholdImpl = parse_threshold(
        """
        must_be_less_than_or_equal: 0
        must_be_greater_than: 2
    """
    )

    assert threshold.get_assertion_summary("m") == "m <= 0 or 2 < m"
    assert threshold.passes(-1)
    assert threshold.passes(0)
    assert not threshold.passes(1)
    assert not threshold.passes(2)
    assert threshold.passes(3)


def parse_threshold(threshold_yaml: str) -> ThresholdImpl:
    dedented_threshold_yaml: str = dedent_and_strip(threshold_yaml)
    yaml_source = YamlSource.from_str(dedented_threshold_yaml)
    yaml_file_content = yaml_source.parse_yaml_file_content()
    yaml_object: YamlObject = yaml_file_content.get_yaml_object()
    threshold_yaml: ThresholdYaml = ThresholdYaml(
        threshold_yaml_object=yaml_object,
    )
    return ThresholdImpl.create(threshold_yaml=threshold_yaml)
