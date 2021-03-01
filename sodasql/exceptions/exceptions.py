#   Copyright 2020 Soda
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

ERROR_CODE_GENERIC = 'generic_error'
ERROR_CODE_CONNECTION_FAILED = 'connection_failed'
ERROR_CODE_AUTHENTICATION_FAILED = 'authentication_failed'
ERROR_CODE_TEST_FAILED = 'test_failed'


class SodaSqlError(Exception):
    """Basic exception for errors raised by soda-sql"""

    def __init__(self, error_code, msg, original_exception):
        super(SodaSqlError, self).__init__(f"{msg}: {str(original_exception)}")
        self.error_code = error_code
        self.original_exception = original_exception


class WarehouseAuthenticationError(SodaSqlError):
    """Basic exception for authentication errors"""

    def __init__(self, error_code, warehouse_type, original_exception):
        super(WarehouseAuthenticationError, self).__init__(error_code,
                                                      f"Soda-sql encountered a problem while trying to authenticate to {warehouse_type}",
                                                           original_exception)
        self.warehouse_type = warehouse_type
        self.original_exception = original_exception


class WarehouseConnectionError(SodaSqlError):
    """Basic exception for authentication errors"""

    def __init__(self, error_code, warehouse_type, original_exception):
        super(WarehouseConnectionError, self).__init__(error_code,
                                                  f"Soda-sql encountered a problem while trying to connect to {warehouse_type}",
                                                       original_exception)
        self.warehouse_type = warehouse_type
        self.original_exception = original_exception


class TestFailureError(SodaSqlError):
    """Basic exception for test errors"""

    def __init__(self, error_code, original_exception):
        super(TestFailureError, self).__init__(error_code, "Soda-sql test failed due to an exception", original_exception)
        self.original_exception = original_exception
