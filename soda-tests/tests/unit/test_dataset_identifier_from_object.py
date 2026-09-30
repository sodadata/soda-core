from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.table_types import FullyQualifiedTableName


class _FakeDialect:
    """Stands in for a SqlDialect: the prefix-index hooks, and the real schema segmenting."""

    schema_name_to_dataset_prefixes = SqlDialect.schema_name_to_dataset_prefixes

    def __init__(self, database_prefix_index, schema_prefix_index):
        self._db = database_prefix_index
        self._schema = schema_prefix_index

    def get_database_prefix_index(self):
        return self._db

    def get_schema_prefix_index(self):
        return self._schema


def test_from_object_postgres_like_includes_database():
    obj = FullyQualifiedTableName(database_name="soda", schema_name="public", table_name="customers")
    di = DatasetIdentifier.from_object("postgres", _FakeDialect(0, 1), obj)
    assert di.to_string() == "postgres/soda/public/customers"


def test_from_object_duckdb_like_drops_catalog():
    obj = FullyQualifiedTableName(database_name="memory", schema_name="main", table_name="t")
    di = DatasetIdentifier.from_object("dd", _FakeDialect(None, 0), obj)
    assert di.to_string() == "dd/main/t"


def test_from_object_no_database_value():
    obj = FullyQualifiedTableName(database_name=None, schema_name="HR", table_name="EMP")
    di = DatasetIdentifier.from_object("ora", _FakeDialect(0, 1), obj)
    assert di.to_string() == "ora/HR/EMP"


def test_from_object_keeps_a_dotted_schema_name_as_one_segment_by_default():
    obj = FullyQualifiedTableName(database_name="soda", schema_name="a.b", table_name="t")
    di = DatasetIdentifier.from_object("postgres", _FakeDialect(0, 1), obj)
    assert di.prefixes == ["soda", "a.b"]


class _PathSplittingDialect(_FakeDialect):
    def schema_name_to_dataset_prefixes(self, schema_name):
        return schema_name.split(".")


def test_from_object_uses_the_dialect_segments_of_the_schema_name():
    obj = FullyQualifiedTableName(database_name=None, schema_name="$scratch.dev_autopilot", table_name="accounts")
    di = DatasetIdentifier.from_object("dremio", _PathSplittingDialect(None, 0), obj)
    assert di.to_string() == "dremio/$scratch/dev_autopilot/accounts"
