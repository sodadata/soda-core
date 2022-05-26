from typing import Optional

from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.location import Location


class UserDefinedFailedRowsExpressionCheckCfg(CheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: Optional[dict],
        location: Location,
        name: str,
        fail_condition_sql_expr: Optional[str],
    ):
        super().__init__(source_header, source_line, source_configurations, location, name)
        self.fail_condition_sql_expr: Optional[str] = fail_condition_sql_expr
