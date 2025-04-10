class DataSourceConnectionException(Exception):
    """Base class for all data source connection exceptions."""

    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)
        self.message = message
