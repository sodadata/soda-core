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
from typing import List, Optional

from sodasql.scan.measurement import Measurement


class Measurements:

    def __init__(self, measurements: List[Measurement]):
        self.measurements = measurements

    def get(self, metric_type: str, column_name: str = None):
        for measurement in self.measurements:
            if measurement.type == metric_type:
                if column_name is None or measurement.column == column_name:
                    return measurement
        raise AssertionError(
            f'No measurement found for metric {metric_type}' +
            (f' and column {column_name}' if column_name else '') + '\n' +
            '\n'.join([str(m) for m in self.measurements]))

    def value(self, metric_type: str, column: str = None):
        return self.get(metric_type, column).value

    def assertValueDataset(self, metric_type: str, value):
        self.assertValue(metric_type, None, value)

    def assertValue(self, metric_type: str, column: Optional[str], value):
        metric_value = self.value(metric_type, column)
        if metric_value != value:
            raise AssertionError(f'Expected {value} for {metric_type}' +
                                 (f'({column})' if column else '') +
                                 f', but was {metric_value}')
