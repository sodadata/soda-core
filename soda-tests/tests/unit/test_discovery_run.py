from soda_core.common.statements.table_types import (
    FullyQualifiedMaterializedViewName,
    FullyQualifiedTableName,
    FullyQualifiedViewName,
    TableType,
)
from soda_core.discovery.discovery_run import DiscoveryRun


class _FakeDialect:
    def get_database_prefix_index(self):
        return 0

    def get_schema_prefix_index(self):
        return 1


class _FakeDataSource:
    """Records the filter kwargs it is called with, and returns a fixed object set."""

    def __init__(self, objects):
        self.name = "postgres"
        self.sql_dialect = _FakeDialect()
        self._objects = objects
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
            "object_types": object_types,
            "include_table_name_like_filters": include_table_name_like_filters,
            "exclude_table_name_like_filters": exclude_table_name_like_filters,
        }
        return self._objects


def _table(table_name):
    return FullyQualifiedTableName(database_name="soda", schema_name="public", table_name=table_name)


def test_maps_objects_to_dqns_and_filters_soda_temp_in_python():
    ds = _FakeDataSource([_table("customers"), _table("orders"), _table("__soda_temp_x")])
    assert DiscoveryRun.execute(ds, prefixes=[]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/orders",
    ]


def test_pushes_include_exclude_down_to_metadata_query():
    ds = _FakeDataSource([_table("customers")])
    DiscoveryRun.execute(ds, prefixes=["soda", "public"], include=["cust%"], exclude=["tmp%"])
    assert ds.received_kwargs["prefixes"] == ["soda", "public"]
    assert ds.received_kwargs["include_table_name_like_filters"] == ["cust%"]
    assert ds.received_kwargs["exclude_table_name_like_filters"] == ["tmp%"]


def test_discovers_views_and_materialized_views_alongside_tables():
    # v3 parity: discovery has no type filter. The explicit object_types are
    # load-bearing — discover_qualified_objects' None default is tables-only,
    # which would silently drop views from discovery.
    ds = _FakeDataSource(
        [
            _table("customers"),
            FullyQualifiedViewName(database_name="soda", schema_name="public", view_name="customers_view"),
            FullyQualifiedMaterializedViewName(
                database_name="soda", schema_name="public", materialized_view_name="customers_mv"
            ),
        ]
    )
    assert DiscoveryRun.execute(ds, prefixes=[]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/customers_view",
        "postgres/soda/public/customers_mv",
    ]
    assert ds.received_kwargs["object_types"] == [TableType.TABLE, TableType.VIEW, TableType.MATERIALIZED_VIEW]
