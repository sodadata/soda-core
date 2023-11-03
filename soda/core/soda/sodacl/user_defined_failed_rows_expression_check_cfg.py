from __future__ import annotations

from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.location import Location
from soda.sodacl.threshold_cfg import ThresholdCfg


class UserDefinedFailedRowsExpressionCheckCfg(CheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: dict | None,
        location: Location,
        name: str,
        fail_condition_sql_expr: str | None,
        samples_limit: int | None = None,
        samples_columns: list | None = None,
        fail_threshold_cfg: ThresholdCfg | None = None,
        warn_threshold_cfg: ThresholdCfg | None = None,
    ):
        super().__init__(
            source_header,
            source_line,
            source_configurations,
            location,
            name,
            samples_limit=samples_limit,
            samples_columns=samples_columns,
        )
        self.fail_condition_sql_expr: str | None = fail_condition_sql_expr
        self.fail_threshold_cfg: ThresholdCfg | None = fail_threshold_cfg
        self.warn_threshold_cfg: ThresholdCfg | None = warn_threshold_cfg
