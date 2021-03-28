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
from typing import List, Optional

from sodasql.scan.test import Test


@dataclass
class SqlMetricYml:

    type: str
    name: Optional[str]
    title: Optional[str]
    sql: str
    index: int
    column_name: Optional[str]
    failed_limit: Optional[int] = None

    # TODO move these next members into a subclass NumericSqlMetricYml
    metric_names: List[str] = None
    group_fields: List[str] = None
    tests: List[Test] = None
