from typing import List

from soda.sodacl.location import Location


class DataSourceCheckCfg:
    def __init__(self, data_source_name: str, location: Location):
        self.data_source_name: str = data_source_name
        self.location: Location = location
        self.include_tables: List[str] = []
        self.exclude_tables: List[str] = []
