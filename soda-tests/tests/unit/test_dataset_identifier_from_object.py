# soda-tests/tests/unit/test_dataset_identifier_from_object.py
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.statements.table_types import FullyQualifiedTableName


class _FakeDialect:
    """Stands in for a SqlDialect: only the two prefix-index hooks are used."""

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
