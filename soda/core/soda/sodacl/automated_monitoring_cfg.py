from typing import List, Optional

from soda.sodacl.location import Location


class AutomatedMonitoringCfg:
    def __init__(self, data_source_name: str, location: Location):
        self.data_source_name: str = data_source_name
        self.location: Location = location
        self.include_tables: Optional[List[str]] = []
        self.exclude_tables: Optional[List[str]] = []
        self.row_count: bool = True
        self.schema: bool = True
        self.min_history = 3
        self.max_history = 90
