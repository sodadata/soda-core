from __future__ import annotations

from typing import Any

from soda_core.common.logging_constants import soda_logger
from soda_core.common.streaming.streaming_consumer import StreamingConsumer

logger = soda_logger


class StreamingActionHandle:
    """Handle returned to producers for streaming data to consumers.

    Supports context manager usage to ensure finish() is always called.
    Consumer failures are logged but never raised to the producer.
    """

    def __init__(self, action: str, consumers: list[StreamingConsumer]):
        self._action = action
        self._consumers = consumers
        self._finished = False

    def new_data(self, data: Any) -> None:
        """Dispatch data to all active consumers. Failures are logged, not raised."""
        for consumer in self._consumers:
            try:
                consumer.process_data(self._action, data)
            except Exception:
                logger.warning(
                    f"Streaming consumer {type(consumer).__name__} failed during process_data for action '{self._action}'",
                    exc_info=True,
                )

    def finish(self) -> None:
        """Signal completion to all consumers. Idempotent."""
        if self._finished:
            return
        self._finished = True
        for consumer in self._consumers:
            try:
                consumer.finish(self._action)
            except Exception:
                logger.warning(
                    f"Streaming consumer {type(consumer).__name__} failed during finish for action '{self._action}'",
                    exc_info=True,
                )

    def __enter__(self) -> StreamingActionHandle:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.finish()
        # Never suppress the producer's exception
        return False
