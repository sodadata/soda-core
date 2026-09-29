import sys
import traceback
from typing import Optional

from soda_core.common.dataset_identifier import DatasetIdentifier


class SodaCoreException(Exception):
    """Base class for all exceptions raised by the soda-core package."""

    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)
        self.message = message


class InvalidArgumentException(SodaCoreException):
    """Indicates an invalid argument was passed to a function or method."""


class ScanExecutionFailedException(SodaCoreException):
    """Raise with a user-facing message for expected/validation failures.

    The exception carries the message; nothing is logged at the raise site.
    The CLI wiring (``cli.handlers.scan.run_scan``) is the single logging
    site: it logs this message clean (no traceback) and reports via
    ``report_scan_execution_failure``. Unexpected failures should propagate
    raw instead — the wiring logs those with the traceback."""


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


class ExtensionException(SodaCoreException):
    """Indicates that the extensions are not installed."""


class FailedContractSkeletonGenerationException(SodaCoreException):
    """Indicates that the contract skeleton generation failed."""


class InvalidRegexException(InvalidContractException):
    """Indicates the regex is invalid."""

    def __init__(self, sql: str):
        super().__init__(f"Invalid regex found in SQL '{sql}'")

    @classmethod
    def should_raise(cls, exception: Exception, sql: str) -> Optional["InvalidRegexException"]:
        # `args` may be empty (e.g. PySpark's AnalysisException), so guard the index access.
        if (
            hasattr(exception, "args")
            and len(exception.args) > 0
            and exception.args[0] == "invalid regular expression: quantifier operand invalid\n"
        ):
            return cls(sql)
        return None


class InvalidDatasetQualifiedNameException(InvalidContractException):
    """Indicates the `dataset` property of the contract is not a valid Dataset Qualified Name"""


class YamlParserException(SodaCoreException):
    """Indicates an error occurred while parsing a YAML file."""

    def __init__(self, message: str, location: Optional[str] = None):
        message_with_location = f"{message}, in {location}" if location else message
        super().__init__(message_with_location)


class ContractParserException(YamlParserException):
    """Indicates an error occurred while parsing a contract."""


class SodaCloudException(SodaCoreException):
    """Base class for all SodaCloud related exceptions."""


class DatasetQueryException(SodaCloudException):
    """A dataset-scoped Soda Cloud query failed.

    The message names the dataset. ``reason`` says why the query failed without naming it, for a
    caller that names the dataset itself.
    """

    def __init__(self, message: str, reason: str):
        super().__init__(message)
        self.reason: str = reason

    def __reduce__(self):
        # Pickle calls the class again with args, which holds only the message, so pass the
        # constructor arguments instead. Each subclass passes its own.
        return type(self), (self.message, self.reason), self.__dict__


class ContractNotFoundException(DatasetQueryException):
    """Indicates the contract was not found in Soda Cloud."""

    def __init__(self, dataset_identifier: DatasetIdentifier):
        super().__init__(
            f"No data contract found for dataset '{dataset_identifier.to_string()}' in Soda Cloud. "
            "Please publish a contract for this dataset in Soda Cloud before proceeding.",
            reason="the dataset has no published contract in Soda Cloud",
        )
        self.dataset_identifier: DatasetIdentifier = dataset_identifier

    def __reduce__(self):
        return type(self), (self.dataset_identifier,), self.__dict__


class DataSourceNotFoundException(DatasetQueryException):
    """Indicates the data source was not found in Soda Cloud."""

    def __init__(self, dataset_identifier: DatasetIdentifier):
        super().__init__(
            f"Data source '{dataset_identifier.data_source_name}' is unknown in Soda Cloud. "
            "Please verify the data source name or configure it in Soda Cloud.",
            reason=f"data source '{dataset_identifier.data_source_name}' is unknown in Soda Cloud",
        )
        self.dataset_identifier: DatasetIdentifier = dataset_identifier

    def __reduce__(self):
        return type(self), (self.dataset_identifier,), self.__dict__


class DatasetNotFoundException(DatasetQueryException):
    """Indicates the dataset was not found in Soda Cloud."""

    def __init__(self, dataset_identifier: DatasetIdentifier):
        super().__init__(
            f"Dataset '{dataset_identifier.dataset_name}' is unknown in Soda Cloud. "
            "Please verify the dataset name or configure it in Soda Cloud.",
            reason="the dataset is unknown in Soda Cloud",
        )
        self.dataset_identifier: DatasetIdentifier = dataset_identifier

    def __reduce__(self):
        return type(self), (self.dataset_identifier,), self.__dict__


class ContractFetchFailedException(SodaCloudException):
    """The contract for a dataset could not be fetched from Soda Cloud, or Soda Cloud returned none.

    The message names the dataset once, as it was given, with the reason. The exception that made
    the fetch fail, if any, is chained as ``__cause__``.
    """

    def __init__(self, dataset_identifier: str, reason: str):
        super().__init__(f"Could not fetch the contract for dataset '{dataset_identifier}': {reason}")
        self.dataset_identifier: str = dataset_identifier
        self.reason: str = reason

    def __reduce__(self):
        # Pickle calls the class again with args, which holds only the message.
        return type(self), (self.dataset_identifier, self.reason), self.__dict__


def get_exception_stacktrace(exception) -> Optional[str]:
    if isinstance(exception, BaseException):
        if sys.version_info < (3, 10):
            return "".join(
                traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)
            )
        return "".join(traceback.format_exception(exception))
    return None
