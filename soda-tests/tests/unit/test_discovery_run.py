from soda_core.common.statements.table_types import FullyQualifiedTableName
from soda_core.discovery.discovery_run import DiscoveryRun


class _FakeDialect:
    def get_database_prefix_index(self):
        return 0

    def get_schema_prefix_index(self):
        return 1


class _FakeDataSource:
    """Records the filter kwargs it is called with, and returns a fixed object set."""

    def __init__(self, object_names):
        self.name = "postgres"
        self.sql_dialect = _FakeDialect()
        self._object_names = object_names
        self.received_kwargs = None

    def discover_qualified_objects(
        self,
        prefixes,
        object_types=None,
        include_table_name_like_filters=None,
        exclude_table_name_like_filters=None,
    ):
        self.received_kwargs = {
            "prefixes": prefixes,
            "include_table_name_like_filters": include_table_name_like_filters,
            "exclude_table_name_like_filters": exclude_table_name_like_filters,
        }
        return [
            FullyQualifiedTableName(database_name="soda", schema_name="public", table_name=n)
            for n in self._object_names
        ]


def test_maps_objects_to_dqns_and_filters_soda_temp_in_python():
    ds = _FakeDataSource(["customers", "orders", "__soda_temp_x"])
    assert DiscoveryRun.execute(ds, prefixes=[]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/orders",
    ]


def test_pushes_include_exclude_down_to_metadata_query():
    ds = _FakeDataSource(["customers"])
    DiscoveryRun.execute(ds, prefixes=["soda", "public"], include=["cust%"], exclude=["tmp%"])
    assert ds.received_kwargs == {
        "prefixes": ["soda", "public"],
        "include_table_name_like_filters": ["cust%"],
        "exclude_table_name_like_filters": ["tmp%"],
    }
