import logging
import os

import pytest
from helpers.test_functions import dedent_and_strip
from soda_core.common.exceptions import YamlParserException
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlList, YamlObject, YamlSource


def test_yaml_api(logs: Logs):
    yaml_source: YamlSource = YamlSource.from_str(
        yaml_str=dedent_and_strip(
            """
        dataset: lskdjflks
        """
        )
    )
    yaml_object: YamlObject = yaml_source.parse()
    assert not logs.has_errors
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
    yaml_object: YamlObject = yaml_source.parse_and_resolve()
    assert not logs.has_errors
    assert yaml_object.read_string("dataset") == "thedataset"


def test_yaml_file(logs: Logs):
    test_yaml_api_file_path = f"{os.path.dirname(__file__)}/test_yaml_api_file.yml"
    yaml_source: YamlSource = YamlSource.from_file_path(file_path=test_yaml_api_file_path)
    yaml_object: YamlObject = yaml_source.parse()
    assert not logs.has_errors
    assert yaml_object.read_string("name") == "thename"


def test_yaml_error_file_not_found(logs: Logs):
    test_yaml_api_file_path = f"{os.path.dirname(__file__)}/unexisting.yml"
    yaml_source: YamlSource = YamlSource.from_file_path(file_path=test_yaml_api_file_path)
    with pytest.raises(YamlParserException) as raised:
        _ = yaml_source.parse()
    assert str(raised.value) == f"YAML file '{test_yaml_api_file_path}' does not exist"
    assert not logs.has_errors


def test_yaml_error_file_is_a_directory(logs: Logs, tmp_path):
    yaml_source: YamlSource = YamlSource.from_file_path(file_path=str(tmp_path))
    with pytest.raises(YamlParserException) as raised:
        _ = yaml_source.parse()
    assert str(raised.value) == f"YAML file '{tmp_path}' is a directory"
    assert not logs.has_errors


@pytest.mark.parametrize(
    "content, error",
    [(b"", "is empty"), ("a: 1\n".encode("utf-16"), "is not UTF-8 text")],
    ids=["empty", "utf-16"],
)
def test_a_file_that_cannot_be_read_raises_its_reason_once(logs: Logs, tmp_path, content, error):
    path = tmp_path / "file.yml"
    path.write_bytes(content)
    with pytest.raises(YamlParserException) as raised:
        YamlSource.from_file_path(file_path=str(path)).parse()
    assert str(raised.value) == f"YAML file '{path}' {error}"
    assert not logs.has_errors


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

    with pytest.raises(YamlParserException, match="YAML file 'yaml_string.yml' root must be an object, but was a list"):
        _ = yaml_source.parse()


def test_yaml_error_empty_yaml_str(logs: Logs):
    yaml_source: YamlSource = YamlSource.from_str(yaml_str="", file_path="yaml_string.yml")
    with pytest.raises(YamlParserException, match="YAML file 'yaml_string.yml' root must be an object, but was empty"):
        _ = yaml_source.parse()


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
    assert not logs.has_errors

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


def test_yaml_syntax_error_message_and_location():
    yaml_source: YamlSource = YamlSource.from_str(yaml_str="a: [1, 2\nb: 3\n", file_path="syntax.yml")
    with pytest.raises(YamlParserException) as raised:
        yaml_source.parse()
    # The location is where the unclosed list starts.
    assert str(raised.value) == "YAML syntax error, in syntax.yml[1,4]"


def test_yaml_duplicate_key_names_the_key_at_its_second_occurrence():
    yaml_source: YamlSource = YamlSource.from_str(yaml_str="a: 1\nb:\n  c: 1\n  c: 2\n", file_path="duplicate.yml")
    with pytest.raises(YamlParserException) as raised:
        yaml_source.parse()
    assert str(raised.value) == 'YAML syntax error: found duplicate key "c", in duplicate.yml[4,3]'


def test_yaml_duplicate_key_message_leaves_out_the_values():
    # The same parser reads data source files, where a duplicated key can hold a password.
    yaml_source: YamlSource = YamlSource.from_str(yaml_str="type: postgres\npassword: first-secret\npassword: s3cret\n")
    with pytest.raises(YamlParserException) as raised:
        yaml_source.parse()
    assert str(raised.value) == 'YAML syntax error: found duplicate key "password", in location[3,1]'


@pytest.mark.parametrize("yaml_str", ["k: \x00\n", "k: !!bool maybe\n", "k: !!int abc\n"], ids=["nul", "bool", "int"])
def test_a_file_ruamel_cannot_load_raises_one_parser_exception(yaml_str):
    yaml_source: YamlSource = YamlSource.from_str(yaml_str=yaml_str, file_path="bad.yml")
    with pytest.raises(YamlParserException) as raised:
        yaml_source.parse()
    assert str(raised.value).startswith("YAML file 'bad.yml' could not be parsed: ")
    assert "\n" not in str(raised.value)


def test_a_load_failure_does_not_quote_the_value():
    # The same parser reads data source files, where the value of a tagged key can be a password.
    yaml_source: YamlSource = YamlSource.from_str(yaml_str="password: !!int s3cret\n", file_path="ds.yml")
    with pytest.raises(YamlParserException) as raised:
        yaml_source.parse()
    assert "s3cret" not in str(raised.value)
    assert raised.value.__cause__ is None and raised.value.__suppress_context__
