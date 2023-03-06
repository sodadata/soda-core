from __future__ import annotations

from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.location import Location


class GroupByCheckCfg(CheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: dict | None,
        location: Location,
        name: str,
        query: str,
        fields: dict,
        checks: list,
        group_limit: int = 1000,
    ):
        super().__init__(source_header, source_line, source_configurations, location, name)
        self.query = query
        self.fields = fields
        self.checks = checks
        self.group_limit = group_limit
