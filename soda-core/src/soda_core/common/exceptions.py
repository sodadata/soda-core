class SodaCoreException(Exception):
    """Base class for all data source connection exceptions."""

    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)
        self.message = message


class InvalidArgumentException(SodaCoreException):
    """Indicates an invalid argument was passed to a function or method."""


class SodaCloudAuthenticationFailedException(SodaCoreException):
    """Indicates the authentication to Soda Cloud failed."""


class InvalidSodaCloudConfigurationException(SodaCoreException):
    """Indicates missing required keys in the Soda Cloud configuration file."""


class InvalidDataSourceConfigurationException(SodaCoreException):
    """Indicates the data source configuration is invalid."""


class DataSourceConnectionException(SodaCoreException):
    """Base class for all data source connection exceptions."""


class InvalidContractException(SodaCoreException):
    """Base class for all invalid contract exceptions."""


class InvalidDatasetQualifiedNameException(InvalidContractException):
    """Indicates the `dataset` property of the contract is not a valid Dataset Qualified Name"""
