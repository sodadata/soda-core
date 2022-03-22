from typing import List

from soda.anomaly_detection.anomaly_detector import AnomalyOutput
from soda.execution.schema_comparator import SchemaComparator
from soda.sodacl.automated_monitoring_cfg import AutomatedMonitoringCfg


class AutomatedMonitoringResult:
    def __init__(self, automated_monitoring_cfg: AutomatedMonitoringCfg):
        self.automated_monitoring_cfg: AutomatedMonitoringCfg = automated_monitoring_cfg

    def get_cloud_diagnostics_dict(self):
        return {}

    def get_log_diagnostic_dict(self) -> dict:
        return {}

    def append_row_count_anomaly_evaluation_result(self, table_name: str, anomaly_output: AnomalyOutput):
        pass

    def append_table_schema_changes(self, schema_comparator: SchemaComparator):
        pass

    def append_table_changes(self, tables_added: List[str], tables_removed: List[str]):
        pass
