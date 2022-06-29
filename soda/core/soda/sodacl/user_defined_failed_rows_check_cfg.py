from typing import Optional

from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.location import Location


class UserDefinedFailedRowsCheckCfg(CheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: Optional[dict],
        location: Location,
        name: Optional[str],
        query: Optional[str],
        samples_limit: Optional[int] = 100,
    ):
        super().__init__(source_header, source_line, source_configurations, location, name)
        self.query: Optional[str] = query
        self.samples_limit = samples_limit
