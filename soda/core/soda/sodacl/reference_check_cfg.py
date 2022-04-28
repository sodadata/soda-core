from typing import List, Optional

from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.location import Location


class ReferenceCheckCfg(CheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: Optional[str],
        location: Location,
        name: Optional[str],
        source_column_names: List[str],
        target_table_name: str,
        target_column_names: List[str],
    ):
        super().__init__(source_header, source_line, source_configurations, location, name)
        self.source_column_names: List[str] = source_column_names
        self.target_table_name: str = target_table_name
        self.target_column_names: List[str] = target_column_names
