import logging
import os

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlList, YamlObject, YamlSource
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_yaml_api(logs: Logs):
    yaml_source: YamlSource = YamlSource.from_str(
        yaml_str=dedent_and_strip(
            """
        dataset: lskdjflks
        """
        )
    )
    yaml_object: YamlObject = yaml_source.parse()
    assert not logs.has_errors()
    assert yaml_object.read_string("dataset") == "lskdjflks"


def test_yaml_resolve_env_vars(env_vars: dict, logs: Logs):
    env_vars["DS"] = "thedataset"
    yaml_source: YamlSource = YamlSource.from_str(
        yaml_str=dedent_and_strip(
            """
        dataset: ${env.DS}
        """
        )
    )
    yaml_source.resolve()
    yaml_object: YamlObject = yaml_source.parse()
    assert not logs.has_errors()
    assert yaml_object.read_string("dataset") == "thedataset"


def test_yaml_file(logs: Logs):
    test_yaml_api_file_path = f"{os.path.dirname(__file__)}/test_yaml_api_file.yml"
    yaml_source: YamlSource = YamlSource.from_file_path(file_path=test_yaml_api_file_path)
    yaml_object: YamlObject = yaml_source.parse()
    assert not logs.has_errors()
    assert yaml_object.read_string("name") == "thename"


def test_yaml_error_file_not_found(logs: Logs):
    test_yaml_api_file_path = f"{os.path.dirname(__file__)}/unexisting.yml"
    yaml_source: YamlSource = YamlSource.from_file_path(file_path=test_yaml_api_file_path)
    yaml_object: YamlObject = yaml_source.parse()
    errors_str = logs.get_errors_str()
    assert "unexisting.yml' does not exist" in errors_str
    assert logs.has_errors()
    assert yaml_object is None


def test_yaml_error_invalid_top_level_element(logs: Logs):
    yaml_source: YamlSource = YamlSource.from_str(
        yaml_str=dedent_and_strip(
            """
        - one
        - two
        """
        ),
        file_path="yaml_string.yml",
    )
    yaml_object: YamlObject = yaml_source.parse()
    errors_str = logs.get_errors_str()
    assert "YAML file 'yaml_string.yml' root must be an object, but was a list" in errors_str
    assert logs.has_errors()
    assert yaml_object is None


def test_yaml_error_empty_yaml_str(logs: Logs):
    yaml_source: YamlSource = YamlSource.from_str(yaml_str="", file_path="yaml_string.yml")
    yaml_object: YamlObject = yaml_source.parse()
    errors_str = logs.get_errors_str()
    assert "YAML file 'yaml_string.yml' root must be an object, but was empty" in errors_str
    assert logs.has_errors()
    assert yaml_object is None


def test_yaml_nested_level(logs: Logs):
    yaml_source: YamlSource = YamlSource.from_str(
        yaml_str=dedent_and_strip(
            """
        level_one:
          level_two:
            - type: item
              name: entry
        """
        )
    )
    yaml_object: YamlObject = yaml_source.parse()
    assert not logs.has_errors()

    level_one_object = yaml_object.read_object("level_one")
    assert isinstance(level_one_object, YamlObject)

    level_two_list = level_one_object.read_list("level_two")
    assert isinstance(level_two_list, YamlList)

    for element in level_two_list:
        assert isinstance(element, YamlObject)
        assert "item" == element.read_string("type")


def test_yaml_locations():
    yaml_str: str = dedent_and_strip(
        """
        one:
          one_two:
            - type: item
              name: entry
        two:
          two_one: a
          two_two: b
        """
    )
    logging.debug(f"\n=== YAML string ============\n{yaml_str}\n============================")
    yaml_source: YamlSource = YamlSource.from_str(yaml_str=yaml_str)
    root_object: YamlObject = yaml_source.parse()

    assert root_object.location is not None
    assert root_object.location.file_path is None
    assert root_object.location.line == 0
    assert root_object.location.column == 0

    value_one: YamlObject = root_object.read_object("one")
    assert value_one.location.line == 1
    assert value_one.location.column == 2

    value_one_two: YamlList = value_one.read_list("one_two")
    assert value_one_two.location.line == 2
    assert value_one_two.location.column == 4

    value_two: YamlObject = root_object.read_object("two")
    assert value_two.location.line == 5
    assert value_two.location.column == 2
