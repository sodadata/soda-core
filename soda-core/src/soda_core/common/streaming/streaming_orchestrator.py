from __future__ import annotations

from typing import Any

from soda_core.common.logging_constants import soda_logger
from soda_core.common.streaming.streaming_action_handle import StreamingActionHandle
from soda_core.common.streaming.streaming_consumer import StreamingConsumer

logger = soda_logger


class StreamingOrchestrator:
    """Module-level registry of streaming consumers (class-level mutable state).

    Same shape as `ContractVerificationHandlerRegistry`: consumers register at plugin-load
    time and stay registered for the process lifetime; producers ask the orchestrator to
    initialize an action and receive a handle that dispatches data to all matching consumers.

    Concurrency: consumers carry per-instance state (e.g. `ReconRowsDiffDwhConsumer` holds
    `_pending_rows`, `_dwh_data_source_impl`, `_context`). The current contract-verification
    flow is sequential, so a single consumer instance is reused across actions without
    overlap. If/when verifications run concurrently, this design must change — either
    consumers must be stateless and key all per-action state on the action handle, or each
    `initialize_action` must hand out fresh consumer instances.
    """

    _consumers: list[StreamingConsumer] = []
    _action_consumers: dict[str, list[StreamingConsumer]] = {}

    @classmethod
    def register_consumer(cls, consumer: StreamingConsumer) -> None:
        cls._consumers.append(consumer)
        cls._rebuild_action_map()

    @classmethod
    def _rebuild_action_map(cls) -> None:
        action_map: dict[str, list[StreamingConsumer]] = {}
        for consumer in cls._consumers:
            for action in consumer.supported_actions():
                action_map.setdefault(action, []).append(consumer)
        cls._action_consumers = action_map

    @classmethod
    def initialize_action(cls, action: str, context: dict[str, Any] | None = None) -> StreamingActionHandle:
        """Initialize matching consumers and return a handle for streaming data.

        Consumers that fail to initialize are excluded from the handle.
        """
        if context is None:
            context = {}

        matching_consumers = cls._action_consumers.get(action, [])
        active_consumers: list[StreamingConsumer] = []

        for consumer in matching_consumers:
            try:
                consumer.initialize(action, context)
                active_consumers.append(consumer)
            except Exception:
                logger.warning(
                    f"Streaming consumer {type(consumer).__name__} failed to initialize for action '{action}'",
                    exc_info=True,
                )

        return StreamingActionHandle(action=action, consumers=active_consumers)

    @classmethod
    def reset(cls) -> None:
        """Clear all registered consumers.

        Primarily used by test fixtures to isolate consumer registration between tests, but
        also valid in any setup that tears down and re-initializes the plugin layer.
        """
        cls._consumers = []
        cls._action_consumers = {}
