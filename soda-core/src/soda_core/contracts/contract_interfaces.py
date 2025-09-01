from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ISodaCloudOutput(ABC):
    """Interface for getting the Soda Cloud output.
    Implement this if you want to convert your object to something that can be sent to Soda Cloud.
    The output of this function depends on the interface that Soda Cloud expects. Can be a scalar value, can be a list, dict, etc.
    We currently expect the interface to be implemented in a class that has the values required to generate the output. But feel free to add a parameter to the function if you want to.
    """

    @abstractmethod
    def get_soda_cloud_output(self) -> Any:
        raise NotImplementedError


class ILoggingOutput(ABC):
    """Interface for getting the logging output.
    Implement this if you want to convert your object to something that can be logged.
    We currently expect the interface to be implemented in a class that has the values required to generate the output. But feel free to add a parameter to the function if you want to.
    The output of this must always be a string!"""

    @abstractmethod
    def get_logging_output(self) -> str:
        raise NotImplementedError
