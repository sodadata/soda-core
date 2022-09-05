from __future__ import annotations

from soda.execution.check.check import Check
from soda.execution.column import Column
from soda.execution.metric.metric import Metric
from soda.execution.partition import Partition
from soda.sodacl.dbt_check_cfg import DbtCheckCfg


class DbtCheck(Check):

    def __init__(
        self,
        check_cfg: DbtCheckCfg,
        identity: str,
        expression: str
    ):
        self.identity = identity
        self.check_cfg = check_cfg
        self.expression = expression
        self.cloud_check_type = "metricThreshold"

    def get_cloud_diagnostics_dict(self) -> dict:
        return {}

    def evaluate(self, metrics: dict[str, Metric], historic_values: dict[str, object]):
        # Not much to evaluate since this is done by dbt
        pass

    def get_cloud_dict(self):
        cloud_dict = {
            "identity": self.identity,
            "identities": {
                # v1 is original without the datasource name and v2 is with datasource name in the hash
                "v1": self.identity,
                "v2": self.identity,
            },
            "name": self.name,
            "type": self.cloud_check_type,
            "definition": self.check_cfg.name,
            "location": "",
            "dataSource": self.data_source_scan.data_source.data_source_name,
            "table": self.check_cfg.table_name,
            "column": self.check_cfg.column_name,
            "metrics": [],
            "outcome": self.outcome.value if self.outcome else None,
            "diagnostics": self.get_cloud_diagnostics_dict(),
        }

        return cloud_dict
