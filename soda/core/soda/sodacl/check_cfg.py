from __future__ import annotations

from copy import deepcopy

from soda.sodacl.location import Location


class CheckCfg:
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: dict | None,
        location: Location,
        name: str | None,
    ):
        self.source_header: str = source_header
        self.source_line: str = source_line
        self.source_configurations = source_configurations
        self.location: Location = location
        self.name: str | None = name

    def get_column_name(self) -> str | None:
        pass

    def instantiate_for_each_table(self, table_alias: str, table_name: str, partition_name: str) -> CheckCfg:
        instantiated_check_cfg = deepcopy(self)
        partition_replace = f" [{partition_name}]" if partition_name else ""
        instantiated_check_cfg.source_header = f"checks for {table_alias} being {table_name}{partition_replace}"
        return instantiated_check_cfg
