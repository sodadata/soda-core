class SodaCloudAuthenticationFailedException(Exception):
    """Indicates the authentication to Soda Cloud failed."""


class InvalidSodaCloudConfigurationException(Exception):
    """Indicates missing required keys in the Soda Cloud configuration file."""


class DataSourceConnectionException(Exception):
    """Base class for all data source connection exceptions."""

    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)
        self.message = message
