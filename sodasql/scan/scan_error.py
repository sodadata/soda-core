from dataclasses import dataclass


@dataclass
class ScanError:
    message: str
    exception: Exception = None

    def __str__(self):
        return f'[{self.get_type()}] {self.get_message()}'

    def get_type(self) -> str:
        return 'error'

    def get_message(self) -> str:
        return self.message


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


@dataclass
class TestFailureScanError(ScanError):
    def get_type(self) -> str:
        return 'test_failure'
