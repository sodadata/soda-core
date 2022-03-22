from __future__ import annotations

from abc import ABC

from soda.common.undefined_instance import undefined
from soda.execution.identity import Identity
from soda.execution.query import Query
from soda.sampler.storage_ref import StorageRef


class Metric(ABC):
    def __init__(
        self,
        data_source_scan: DataSourceScan,
        partition: Partition | None,
        column: Column | None,
        name: str,
        check: Check,
        # schedule name, data source name, table name, partition name, column name and name are
        # already added to the identity.  All other metric configurations that are part of the
        # identity should be provided in this identity_hash_parts list.
        # Only a combination of list, dict, str, int, float is allowed.
        identity_parts: list,
    ):
        from soda.execution.column import Column
        from soda.execution.partition import Partition

        # Only used in the string representation of metrics.  See __str__(self) below
        self.name: str = name
        # identity will be used to resolve the same metric and bind it to different checks.
        # Unique within an organisation, but cannot be used as global id in Soda Cloud.
        self.identity: str = Identity.create_identity(
            "metric", data_source_scan, partition, column, name, identity_parts
        )
        self.data_source_scan = data_source_scan
        self.partition: Partition = partition
        self.column: Column | None = column

        self.checks: set[Check] = {check}

        self.value: object = undefined
        self.queries: list[Query] = []
        self.formula_values: dict[str, object] = None
        self.failed_rows_storage_ref: StorageRef | None = None
        self.duplicate_frequencies_storage_ref: StorageRef | None = None

    def __eq__(self, other: Metric) -> bool:
        if self is other:
            return True
        if type(self) != type(other):
            return False
        return self.identity == other.identity

    def __hash__(self) -> int:
        return hash(self.identity)

    def __str__(self):
        return self.name

    def merge_checks(self, other_metric: Metric):
        if other_metric.checks is not None:
            self.checks.update(other_metric.checks)

    def set_value(self, value):
        self.value = value

    def ensure_query(self):
        """
        Every query should be added to either to the partition.queries or to data_source_scan.queries
        """

    def get_cloud_dict(self):
        pass

        return {
            "identity": self.identity,
            "metricName": self.name,
            "value": self.value,
            # TODO: re-enable once backend supports these properties.
            # "dataSourceName": self.data_source_scan.data_source.data_source_name,
            # "tableName": Partition.get_table_name(self.partition),
            # "filterName": Partition.get_partition_name(self.partition),
            # "columnName": Column.get_partition_name(self.column),
        }
