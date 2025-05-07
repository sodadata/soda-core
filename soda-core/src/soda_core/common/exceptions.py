from soda_core.common.dataset_identifier import DatasetIdentifier


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


class SodaCloudException(SodaCoreException):
    """Base class for all SodaCloud related exceptions."""


class ContractNotFoundException(SodaCloudException):
    """Indicates the contract was not found in Soda Cloud."""

    def __init__(self, dataset_identifier: DatasetIdentifier):
        super().__init__(
            f"No data contract found for dataset '{str(dataset_identifier)}' in Soda Cloud. "
            "Please publish a contract for this dataset in Soda Cloud before proceeding."
        )


class DataSourceNotFoundException(SodaCloudException):
    """Indicates the data source was not found in Soda Cloud."""

    def __init__(self, dataset_identifier: DatasetIdentifier):
        super().__init__(
            f"Data source '{dataset_identifier.data_source_name}' is unknown in Soda Cloud. "
            "Please verify the data source name or configure it in Soda Cloud."
        )


class DatasetNotFoundException(SodaCloudException):
    """Indicates the dataset was not found in Soda Cloud."""

    def __init__(self, dataset_identifier: DatasetIdentifier):
        super().__init__(
            f"Dataset '{dataset_identifier.dataset_name}' is unknown in Soda Cloud. "
            "Please verify the dataset name or configure it in Soda Cloud."
        )
