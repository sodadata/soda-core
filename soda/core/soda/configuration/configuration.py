from __future__ import annotations

from soda.common.file_system import file_system
from soda.execution.telemetry import Telemetry
from soda.sampler.log_sampler import LogSampler
from soda.sampler.sampler import Sampler
from soda.scan import Scan
from soda.soda_cloud.soda_cloud import SodaCloud
from soda.sodacl.format_cfg import FormatCfg


class Configuration:
    def __init__(self, scan: Scan):
        self.scan = scan
        self.connection_properties_by_name: dict[str, dict] = {}
        self.data_source_properties_by_name: dict[str, dict] = {}
        self.format_cfgs: dict[str, str] = FormatCfg.default_formats
        self.telemetry: Telemetry | None = Telemetry()
        self.soda_cloud: SodaCloud | None = None
        self.file_system = file_system()
        self.sampler: Sampler | None = LogSampler()

    def add_spark_session(self, data_source_name: str, spark_session):
        self.connection_properties_by_name["provided_spark"] = {
            "type": "provided_spark",
            "provided_spark_session": spark_session,
        }
        self.data_source_properties_by_name[data_source_name] = {"connection": "provided_spark"}
