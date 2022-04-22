ERROR_CODE_GENERIC = "generic_error"
ERROR_CODE_CONNECTION_FAILED = "connection_failed"


class SodaSqlError(Exception):
    def __init__(self, msg, original_exception):
        super().__init__(f"{msg}: {str(original_exception)}")
        self.error_code = ERROR_CODE_GENERIC
        self.original_exception = original_exception


class DataSourceError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.error_code = ERROR_CODE_CONNECTION_FAILED


class DataSourceConnectionError(SodaSqlError):
    def __init__(self, data_source_type, original_exception):
        super().__init__(
            f"Encountered a problem while trying to connect to {data_source_type}",
            original_exception,
        )
        self.error_code = ERROR_CODE_CONNECTION_FAILED
        self.data_source_type = data_source_type
