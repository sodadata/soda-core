from typing import List, Optional

from soda.sodacl.location import Location
from soda.sodacl.metric_check_cfg import MetricCheckCfg
from soda.sodacl.missing_and_valid_cfg import MissingAndValidCfg
from soda.sodacl.threshold_cfg import ThresholdCfg


class AnomalyMetricCheckCfg(MetricCheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: Optional[str],
        location: Location,
        name: Optional[str],
        metric_name: str,
        metric_args: Optional[List[object]],
        missing_and_valid_cfg: Optional[MissingAndValidCfg],
        filter: Optional[str],
        condition: Optional[str],
        metric_expression: Optional[str],
        metric_query: Optional[str],
        change_over_time_cfg: Optional["ChangeOverTimeCfg"],
        fail_threshold_cfg: Optional[ThresholdCfg],
        warn_threshold_cfg: Optional[ThresholdCfg],
    ):
        super().__init__(
            source_header,
            source_line,
            source_configurations,
            location,
            name,
            metric_name,
            metric_args,
            missing_and_valid_cfg,
            filter,
            condition,
            metric_expression,
            metric_query,
            change_over_time_cfg,
            fail_threshold_cfg,
            warn_threshold_cfg,
        )
