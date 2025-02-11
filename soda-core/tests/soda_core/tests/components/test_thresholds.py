from soda_core.common.yaml import YamlSource, YamlObject
from soda_core.contracts.impl.contract_verification_impl import Threshold, Column
from soda_core.contracts.impl.contract_yaml import CheckYaml, ThresholdCheckYaml
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_threshold_must_be():
    threshold: Threshold = parse_threshold("""
        must_be: 0
    """)

    assert threshold.get_assertion_summary("m") == "m = 0"
    assert not threshold.passes(-1)
    assert threshold.passes(0)
    assert not threshold.passes(1)


def test_threshold_greater_than():
    threshold: Threshold = parse_threshold("""
        must_be_greater_than: 0
    """)

    assert threshold.get_assertion_summary("m") == "0 < m"
    assert not threshold.passes(-1)
    assert not threshold.passes(0)
    assert threshold.passes(1)


def test_threshold_between():
    threshold: Threshold = parse_threshold("""
        must_be_between: [0, 1]
    """)

    assert threshold.get_assertion_summary("m") == "0 <= m <= 1"
    assert not threshold.passes(-1)
    assert threshold.passes(0)
    assert threshold.passes(1)
    assert not threshold.passes(2)


def parse_threshold(threshold_yaml: str) -> Threshold:
    dedented_threshold_yaml: str = dedent_and_strip(threshold_yaml)
    yaml_source = YamlSource.from_str(dedented_threshold_yaml)
    yaml_file_content = yaml_source.parse_yaml_file_content()
    yaml_object: YamlObject = yaml_file_content.get_yaml_object()
    check_yaml: ThresholdCheckYaml = ThresholdCheckYaml(check_yaml_object=yaml_object)
    return Threshold.create(check_yaml)
