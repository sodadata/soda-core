from __future__ import annotations

from soda.cloud.dbt_config import DbtCloudConfig
from soda.common.file_system import file_system
from soda.execution.telemetry import Telemetry
from soda.sampler.default_sampler import DefaultSampler
from soda.sampler.sampler import Sampler
from soda.scan import Scan
from soda.soda_cloud.soda_cloud import SodaCloud


class Configuration:
    def __init__(self, scan: Scan):
        self.scan = scan
        self.data_source_properties_by_name: dict[str, dict] = {}
        self.telemetry: Telemetry | None = Telemetry()
        self.soda_cloud: SodaCloud | None = None
        self.file_system = file_system()
        self.sampler: Sampler = DefaultSampler()
        self.dbt_cloud: DbtCloudConfig | None = None

    def add_spark_session(self, data_source_name: str, spark_session):
        self.data_source_properties_by_name[data_source_name] = {
            "type": "spark_df",
            "connection": "spark_df_data_source",
            "spark_session": spark_session,
        }
