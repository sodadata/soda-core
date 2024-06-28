from __future__ import annotations

from soda.cloud.soda_cloud import SodaCloud as SodaCLSodaCloud
from soda.common.logs import Logs
from soda.scan import Scan


class CustomizedSodaClCloud(SodaCLSodaCloud):
    def __init__(
        self,
        host: str,
        api_key_id: str,
        api_key_secret: str,
        token: str | None,
        port: str | None,
        logs: Logs,
        scheme: str,
        default_data_source_properties: dict,
    ):
        super().__init__(host, api_key_id, api_key_secret, token, port, logs, scheme)
        self.default_data_source_properties: dict = default_data_source_properties

    def send_scan_results(self, scan: Scan):
        scan_results = self.build_scan_results(scan)
        scan_results["type"] = "sodaCoreInsertScanResults"
        scan_results["defaultDataSourceProperties"] = self.default_data_source_properties
        return self._execute_command(scan_results, command_name="send_scan_results")
