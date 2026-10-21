import pytest
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import InvalidDatasetQualifiedNameException
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource


def test_parse_splits_data_source_prefixes_and_dataset():
    identifier = DatasetIdentifier.parse("ds/db/schema/table")
    assert (identifier.data_source_name, identifier.prefixes, identifier.dataset_name) == (
        "ds",
        ["db", "schema"],
        "table",
    )


@pytest.mark.parametrize(
    "dataset_qualified_name, error",
    [
        (None, "Dataset DQN must be a valid string and cannot be None"),
        ("", "Dataset DQN must not be empty"),
        ("table", "Dataset DQN must contain at least a data source and a dataset"),
        ("/db/table", "Dataset DQN must not start or end with '/' or have an empty name between two '/'"),
        ("ds/db/", "Dataset DQN must not start or end with '/' or have an empty name between two '/'"),
        ("ds//table", "Dataset DQN must not start or end with '/' or have an empty name between two '/'"),
        ("ds/" + "t" * 2046, "Dataset DQN must be at most 2048 characters"),
        ("ds/ta\tble", "Dataset DQN must not contain control characters"),
        ("ds/table\x7f", "Dataset DQN must not contain control characters"),
    ],
    ids=["none", "empty", "one name", "leading slash", "trailing slash", "empty name", "too long", "tab", "delete"],
)
def test_parse_refuses_what_soda_cloud_refuses(dataset_qualified_name, error):
    with pytest.raises(InvalidDatasetQualifiedNameException) as raised:
        DatasetIdentifier.parse(dataset_qualified_name)
    assert str(raised.value) == error


def test_parse_takes_a_name_of_2048_characters():
    assert DatasetIdentifier.parse("ds/" + "t" * 2045).dataset_name == "t" * 2045


def test_the_reader_logs_the_reason_and_returns_none(logs: Logs):
    yaml_object = YamlSource.from_str(yaml_str="dataset: ds//table\n", file_path="c.yml").parse()
    assert yaml_object.read_dataset_identifier("dataset") is None
    assert logs.get_errors() == [
        "Invalid dataset qualified name in 'dataset': 'ds//table'. "
        "Dataset DQN must not start or end with '/' or have an empty name between two '/'"
    ]
