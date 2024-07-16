from __future__ import annotations

from copy import deepcopy

from soda.sodacl.location import Location


class CheckCfg:
    """Represents the config from the check config (yaml or otherwise) as closely as possible."""

    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: dict | None,
        location: Location,
        name: str | None,
        samples_limit: int | None = None,
        samples_columns: list | None = None,
        failed_rows_query: str | None = None,
    ):
        self.source_header: str = source_header
        self.source_line: str = source_line
        self.source_configurations = source_configurations
        self.location: Location = location
        self.name: str | None = name
        self.samples_limit: int | None = samples_limit
        self.samples_columns: list | None = samples_columns
        self.failed_rows_query: str | None = failed_rows_query

    def get_column_name(self) -> str | None:
        pass

    def instantiate_for_each_dataset(
        self, name: str, table_alias: str, table_name: str, partition_name: str
    ) -> CheckCfg:
        instantiated_check_cfg = deepcopy(self)
        partition_replace = f" [{partition_name}]" if partition_name else ""
        instantiated_check_cfg.name = name
        instantiated_check_cfg.source_header = f"checks for {table_alias} being {table_name}{partition_replace}"
        return instantiated_check_cfg
