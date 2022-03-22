from typing import Dict, List

from soda.sodacl.automated_monitoring_cfg import AutomatedMonitoringCfg
from soda.sodacl.check_cfg import CheckCfg
from soda.sodacl.table_cfg import TableCfg


class DataSourceScanCfg:
    def __init__(self, data_source_name: str):
        self.data_source_name: str = data_source_name
        self.tables_cfgs: Dict[str, TableCfg] = {}
        self.monitoring_cfgs: List[AutomatedMonitoringCfg] = []
        self.check_cfgs: List[CheckCfg] = []

    def get_or_create_table_cfg(self, table_name) -> TableCfg:
        table_cfg = self.tables_cfgs.get(table_name)
        if not table_cfg:
            table_cfg = TableCfg(table_name)
            self.tables_cfgs[table_name] = table_cfg
        return table_cfg

    def add_monitoring_cfg(self, monitoring_cfg: AutomatedMonitoringCfg):
        self.monitoring_cfgs.append(monitoring_cfg)

    def add_check_cfg(self, check_cfg: CheckCfg):
        self.check_cfgs.append(check_cfg)
