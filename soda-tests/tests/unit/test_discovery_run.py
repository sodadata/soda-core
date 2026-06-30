# soda-tests/tests/unit/test_discovery_run.py
from soda_core.common.statements.table_types import FullyQualifiedTableName
from soda_core.discovery.discovery_run import DiscoveryRun


class _FakeDialect:
    def get_database_prefix_index(self):
        return 0

    def get_schema_prefix_index(self):
        return 1


class _FakeDataSource:
    """Minimal DataSourceImpl stand-in: name, sql_dialect, discover_qualified_objects."""

    def __init__(self, object_names):
        self.name = "postgres"
        self.sql_dialect = _FakeDialect()
        self._object_names = object_names

    def discover_qualified_objects(self, prefixes, object_types=None):
        return [
            FullyQualifiedTableName(database_name="soda", schema_name="public", table_name=n)
            for n in self._object_names
        ]


def test_builds_dqns_and_filters_soda_temp():
    ds = _FakeDataSource(["customers", "orders", "__soda_temp_x"])
    assert DiscoveryRun.execute(ds, prefixes=[]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/orders",
    ]


def test_include_exclude_patterns():
    ds = _FakeDataSource(["customers", "orders", "cust_archive"])
    assert DiscoveryRun.execute(ds, prefixes=[], include=["cust%"]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/cust_archive",
    ]
    assert DiscoveryRun.execute(ds, prefixes=[], exclude=["cust%"]) == [
        "postgres/soda/public/orders",
    ]
