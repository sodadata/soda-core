import os

from soda_core.common.yaml import YamlFile, YamlObject, YamlList
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_yaml_api():
    yaml_file: YamlFile = YamlFile.from_str(dedent_and_strip(
        """
        dataset: lskdjflks
        """
    ))
    yaml_object: YamlObject = yaml_file.parse()
    assert not yaml_file.has_errors()
    assert yaml_object.read_string("dataset") == "lskdjflks"


def test_yaml_resolve_env_vars(env_vars: dict):
    env_vars["DS"] = "thedataset"
    yaml_file: YamlFile = YamlFile.from_str(dedent_and_strip(
        """
        dataset: ${DS}
        """
    ))
    yaml_file.parse()
    yaml_object: YamlObject = yaml_file.parse()
    assert not yaml_file.has_errors()
    assert yaml_object.read_string("dataset") == "thedataset"


def test_yaml_file():
    test_yaml_api_file_path = f"{os.path.dirname(__file__)}/test_yaml_api_file.yml"
    yaml_file: YamlFile = YamlFile.from_file_path(yaml_file_path=test_yaml_api_file_path)
    yaml_object: YamlObject = yaml_file.parse()
    yaml_file.assert_no_errors()
    assert yaml_object.read_string("name") == "thename"


def test_yaml_error_file_not_found():
    test_yaml_api_file_path = f"{os.path.dirname(__file__)}/unexisting.yml"
    yaml_file: YamlFile = YamlFile.from_file_path(yaml_file_path=test_yaml_api_file_path)
    yaml_object: YamlObject = yaml_file.parse()
    errors_str = yaml_file.logs.get_errors_str()
    assert "unexisting.yml' does not exist" in errors_str
    assert yaml_file.has_errors()
    assert yaml_object is None


def test_yaml_error_invalid_top_level_element():
    yaml_file: YamlFile = YamlFile.from_str(dedent_and_strip(
        """
        - one
        - two
        """
    ))
    yaml_object: YamlObject = yaml_file.parse()
    errors_str = yaml_file.logs.get_errors_str()
    assert "Expected top level YAML object in provided YAML str" in errors_str
    assert yaml_file.has_errors()
    assert yaml_object is None


def test_yaml_nested_level():
    yaml_file: YamlFile = YamlFile.from_str(dedent_and_strip(
        """
        level_one:
          level_two:
            - type: item
              name: entry
        """
    ))
    yaml_object: YamlObject = yaml_file.parse()
    yaml_file.assert_no_errors()

    level_one_object = yaml_object.read_object("level_one")
    assert isinstance(level_one_object, YamlObject)

    level_two_list = level_one_object.read_list("level_two")
    assert isinstance(level_two_list, YamlList)

    for element in level_two_list:
        assert isinstance(element, YamlObject)
        assert "item" == element.read_string("type")
