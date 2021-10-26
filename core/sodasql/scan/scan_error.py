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
from deprecated import deprecated
from sodasql.scan.test import Test


@dataclass
class ScanError:
    message: str
    exception: Exception = None

    def __str__(self) -> str:
        return f'[{self.get_type()}] {self.get_message()}'

    def to_dict(self) -> dict:
        json = {
            'type': self.get_type(),
            'message': self.get_message()
        }
        if self.exception is not None:
            json['exception'] = str(self.exception)

            if hasattr(self.exception, "error_code"):
                json['errorCode'] = self.exception.error_code
        return json

    @deprecated(version='2.1.0b19', reason='This function is deprecated, please use to_dict')
    def to_json(self):
        return self.to_dict()

    def get_type(self) -> str:
        return 'error'

    def get_message(self) -> str:
        return self.message


@dataclass
class TestExecutionScanError(ScanError):
    test: Test = None

    def get_type(self) -> str:
        return 'test_execution_error'


@dataclass
class SodaCloudScanError(ScanError):
    def get_type(self) -> str:
        return 'soda_cloud_error'


@dataclass
class WarehouseAuthenticationScanError(ScanError):
    def get_type(self) -> str:
        return 'warehouse_authentication_error'


@dataclass
class WarehouseConnectionScanError(ScanError):
    def get_type(self) -> str:
        return 'warehouse_connection_error'
