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
from dataclasses import dataclass
from typing import Set, List

from sodasql.scan.missing import Missing
from sodasql.scan.sql_metric_yml import SqlMetricYml
from sodasql.scan.test import Test
from sodasql.scan.validity import Validity


@dataclass
class ScanYmlColumn:

    metrics: Set[str]
    sql_metric_ymls: List[SqlMetricYml]
    missing: Missing
    validity: Validity
    tests: List[Test]
