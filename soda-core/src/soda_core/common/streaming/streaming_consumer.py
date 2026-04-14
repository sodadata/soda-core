from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class StreamingConsumer(ABC):
    """Abstract base class for streaming consumers.

    Consumers register with the StreamingOrchestrator and receive data
    from producers during check execution. This allows post-processing
    handlers (like DWH) to receive data in-flight without re-executing
    the check.
    """

    @abstractmethod
    def supported_actions(self) -> list[str]:
        """Return the action names this consumer handles (e.g. "recon_check_differences")."""
        pass

    @abstractmethod
    def initialize(self, action: str, context: dict[str, Any]) -> None:
        """Set up for receiving data (e.g. open connection, create table)."""
        pass

    @abstractmethod
    def process_data(self, action: str, data: Any) -> None:
        """Handle one piece of data from a producer."""
        pass

    @abstractmethod
    def finish(self, action: str) -> None:
        """Clean up after streaming is done (e.g. flush, close connection)."""
        pass
