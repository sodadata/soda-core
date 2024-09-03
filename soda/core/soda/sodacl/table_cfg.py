from __future__ import annotations

from soda.sodacl.partition_cfg import PartitionCfg


class TableCfg:
    def __init__(self, table_name: str):
        self.table_name: str = table_name
        self.attributes: dict | None = None
        self.partition_cfgs: list[PartitionCfg] = []

    def create_partition(self, file_path: str, partition_name: str) -> PartitionCfg:
        # Because the default table level checks are modelled with filter_name = None
        if partition_name is None:
            # file paths should be ignored
            file_path = None
        partition_cfg = PartitionCfg(file_path=file_path, partition_name=partition_name)
        self.partition_cfgs.append(partition_cfg)
        return partition_cfg

    def find_partition(self, file_path: str, partition_name: str) -> PartitionCfg:
        # Because the default table level checks are modelled with filter_name = None
        if partition_name is None:
            # file paths should be ignored
            file_path = None
        partition_cfg = next(
            (
                filter
                for filter in self.partition_cfgs
                if filter.file_path == file_path and filter.partition_name == partition_name
            ),
            None,
        )
        # Because the default table level checks are modelled with filter_name = None
        if partition_cfg is None and partition_name is None:
            return self.create_partition(None, None)
        return partition_cfg
