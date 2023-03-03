from __future__ import annotations


class GroupByCfg:
    def __init__(
        self,
        query: str,
        fields: dict,
        checks: list,
        group_limit: int = 1000,
    ):
        self.query: query
        self.fields = fields
        self.checks = checks
        self.group_limit = group_limit
