from dataclasses import dataclass

from sodasql.scan.test import Test


@dataclass
class ScanError:
    message: str
    exception: Exception = None

    def __str__(self) -> str:
        return f'[{self.get_type()}] {self.get_message()}'

    def to_json(self) -> dict:
        json = {
            'type': self.get_type(),
            'message': self.get_message()
        }
        if self.exception is not None:
            json['exception'] = str(self.exception)
        return json

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
