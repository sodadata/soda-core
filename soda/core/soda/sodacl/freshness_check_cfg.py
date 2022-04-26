from datetime import timedelta
from typing import Optional

from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.location import Location


class FreshnessCheckCfg(CheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: Optional[str],
        location: Location,
        name: Optional[str],
        column_name: str,
        variable_name: str,
        fail_staleness_threshold: timedelta,
        warn_staleness_threshold: timedelta,
    ):
        super().__init__(source_header, source_line, source_configurations, location, name)
        self.column_name: str = column_name
        self.variable_name: str = variable_name
        self.fail_staleness_threshold: timedelta = fail_staleness_threshold
        self.warn_staleness_threshold: timedelta = warn_staleness_threshold

    def get_column_name(self) -> Optional[str]:
        return self.column_name
