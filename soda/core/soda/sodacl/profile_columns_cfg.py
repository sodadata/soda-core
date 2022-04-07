from typing import List, Optional

from soda.sodacl.location import Location


class ProfileColumnsCfg:
    def __init__(self, data_source_name: str, location: Location):
        self.data_source_name: str = data_source_name
        self.location: Location = location
        self.include_columns: Optional[List[str]] = []
        self.exclude_columns: Optional[List[str]] = []
