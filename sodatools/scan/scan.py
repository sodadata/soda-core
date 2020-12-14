#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from typing import List

from sodatools.scan.custom_metric import CustomMetric
from sodatools.scan.scan_configuration import ScanConfiguration
from sodatools.soda_client.soda_client import SodaClient
from sodatools.sql_store.sql_store import SqlStore


class Scan:

    def __init__(self,
                 sql_store: SqlStore,
                 scan_configuration: ScanConfiguration = None,
                 custom_metrics: List[CustomMetric] = None,
                 soda_client: SodaClient = None):
        self.soda_client: SodaClient = soda_client
        self.sql_store: SqlStore = sql_store
        self.scan_configuration: ScanConfiguration = scan_configuration
        self.custom_metrics: List[CustomMetric] = custom_metrics

    def execute(self):
        return self.sql_store.scan(scan_configuration=self.scan_configuration)
