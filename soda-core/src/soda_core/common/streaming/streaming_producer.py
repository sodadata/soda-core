from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from soda_core.common.streaming.streaming_action_handle import StreamingActionHandle
from soda_core.common.streaming.streaming_orchestrator import StreamingOrchestrator


class StreamingProducer(ABC):
    """Abstract base class for streaming producers (mixin).

    Producers are NOT registered with the orchestrator. This ABC exists for
    self-documentation, validation, and generalizability. Checks use it as a mixin.
    """

    @abstractmethod
    def produced_actions(self) -> list[str]:
        """Declare what actions this producer generates."""

    def initialize_action(self, action: str, context: dict[str, Any] | None = None) -> StreamingActionHandle:
        """Convenience method that validates the action and delegates to StreamingOrchestrator."""
        produced_actions = self.produced_actions()
        if action not in produced_actions:
            raise ValueError(
                f"{type(self).__name__} does not produce action '{action}'. "
                f"Produced actions: {produced_actions}"
            )
        return StreamingOrchestrator.initialize_action(action, context)
