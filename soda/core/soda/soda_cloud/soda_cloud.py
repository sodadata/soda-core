import logging
from typing import Dict, Set

from soda.common.json_helper import JsonHelper
from soda.soda_cloud.historic_descriptor import HistoricDescriptor
from soda.soda_cloud.soda_cloud_client import SodaCloudClient

logger = logging.getLogger(__name__)


class SodaCloud:
    def __init__(self, host: str, api_key_id: str, api_key_secret: str):
        host = host if isinstance(host, str) else "cloud.soda.io"
        self.api_key_id = api_key_id
        self.api_key_secret = api_key_secret
        self.cloud_client = SodaCloudClient(api_key_id, api_key_secret, host)

    def get(self, historic_query):
        # TODO unify this with get_historic_data
        pass

    def get_historic_data(self, historic_descriptors: Set["HistoricDescriptor"]) -> Dict[HistoricDescriptor, object]:
        # TODO Need to check filtering issue for the data query
        pass

    def send_scan_results(self, scan: "Scan"):
        scan_results = self.build_scan_results(scan)
        self.cloud_client.insert_scan_results(scan_results)

    @staticmethod
    def build_scan_results(scan):
        return JsonHelper.to_jsonnable(
            {
                "definitionName": scan._scan_definition_name,
                # TODO Data timestamp should be coming from commandline params?
                "dataTimestamp": scan._data_timestamp,
                "scanStartTimestamp": scan._scan_start_timestamp,
                "scanEndTimestamp": scan._scan_end_timestamp,
                "hasErrors": scan.has_error_logs(),
                "hasWarnings": scan.has_check_warns(),
                "hasFailures": scan.has_check_fails(),
                "metrics": [metric.get_cloud_dict() for metric in scan._metrics],
                "checks": [check.get_cloud_dict() for check in scan._checks],
                # TODO Queries are not supported by Soda Cloud yet.
                # "queries": [query.get_cloud_dict() for query in scan._queries],
            }
        )

    # TODO consider adding memory usage to SodaCloud as a goodie
    @staticmethod
    def memory_usage_psutil():
        """
        returns the memory usage in MB
        """
        # http://fa.bianp.net/blog/2013/different-ways-to-get-memory-consumption-or-lessons-learned-from-memory_profiler/
        import psutil

        process = psutil.Process(os.getpid())
        mem = process.get_memory_info()[0] / float(2**20)
        return mem
