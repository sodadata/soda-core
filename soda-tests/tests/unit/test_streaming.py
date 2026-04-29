from __future__ import annotations

from typing import Any

import pytest
from soda_core.common.streaming import (
    StreamingActionHandle,
    StreamingConsumer,
    StreamingOrchestrator,
    StreamingProducer,
)


class MockConsumer(StreamingConsumer):
    def __init__(self, actions: list[str]):
        self._actions = actions
        self.initialized: list[tuple[str, dict]] = []
        self.received_data: list[tuple[str, Any]] = []
        self.finished_actions: list[str] = []

    def supported_actions(self) -> list[str]:
        return self._actions

    def initialize(self, action: str, context: dict[str, Any]) -> None:
        self.initialized.append((action, context))

    def process_data(self, action: str, data: Any) -> None:
        self.received_data.append((action, data))

    def finish(self, action: str) -> None:
        self.finished_actions.append(action)


class FailingConsumer(StreamingConsumer):
    """Consumer that fails at a configurable stage."""

    def __init__(self, actions: list[str], fail_on: str = ""):
        self._actions = actions
        self._fail_on = fail_on

    def supported_actions(self) -> list[str]:
        return self._actions

    def initialize(self, action: str, context: dict[str, Any]) -> None:
        if self._fail_on == "initialize":
            raise RuntimeError("initialize failed")

    def process_data(self, action: str, data: Any) -> None:
        if self._fail_on == "process_data":
            raise RuntimeError("process_data failed")

    def finish(self, action: str) -> None:
        if self._fail_on == "finish":
            raise RuntimeError("finish failed")


class MockProducer(StreamingProducer):
    def __init__(self, actions: list[str]):
        self._actions = actions

    def produced_actions(self) -> list[str]:
        return self._actions


class TestStreamingNoConsumers:
    def test_no_consumers_handle_works_as_noop(self):
        handle = StreamingOrchestrator.initialize_action("some_action", {})
        handle.new_data({"key": "value"})
        handle.finish()


class TestStreamingSingleConsumer:
    def test_single_consumer_lifecycle(self):
        consumer = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(consumer)

        ctx = {"key": "val"}
        handle = StreamingOrchestrator.initialize_action("my_action", ctx)
        assert len(consumer.initialized) == 1
        assert consumer.initialized[0] == ("my_action", ctx)

        handle.new_data({"row": 1})
        handle.new_data({"row": 2})
        assert len(consumer.received_data) == 2
        assert consumer.received_data[0] == ("my_action", {"row": 1})
        assert consumer.received_data[1] == ("my_action", {"row": 2})

        handle.finish()
        assert consumer.finished_actions == ["my_action"]


class TestStreamingMultipleConsumers:
    def test_multiple_consumers_same_action(self):
        consumer1 = MockConsumer(["shared_action"])
        consumer2 = MockConsumer(["shared_action"])
        StreamingOrchestrator.register_consumer(consumer1)
        StreamingOrchestrator.register_consumer(consumer2)

        handle = StreamingOrchestrator.initialize_action("shared_action", {})
        handle.new_data("data1")
        handle.finish()

        assert len(consumer1.received_data) == 1
        assert len(consumer2.received_data) == 1
        assert consumer1.finished_actions == ["shared_action"]
        assert consumer2.finished_actions == ["shared_action"]

    def test_consumer_for_different_action_not_invoked(self):
        consumer_a = MockConsumer(["action_a"])
        consumer_b = MockConsumer(["action_b"])
        StreamingOrchestrator.register_consumer(consumer_a)
        StreamingOrchestrator.register_consumer(consumer_b)

        handle = StreamingOrchestrator.initialize_action("action_a", {})
        handle.new_data("data")
        handle.finish()

        assert len(consumer_a.received_data) == 1
        assert len(consumer_b.received_data) == 0
        assert consumer_b.initialized == []
        assert consumer_b.finished_actions == []


class TestStreamingConsumerFailures:
    def test_consumer_fails_during_initialize_excluded(self):
        failing = FailingConsumer(["my_action"], fail_on="initialize")
        good = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(failing)
        StreamingOrchestrator.register_consumer(good)

        handle = StreamingOrchestrator.initialize_action("my_action", {})
        handle.new_data("data")
        handle.finish()

        # Good consumer received data; failing one was excluded
        assert len(good.received_data) == 1
        assert good.finished_actions == ["my_action"]

    def test_consumer_fails_during_process_data_others_proceed(self):
        failing = FailingConsumer(["my_action"], fail_on="process_data")
        good = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(failing)
        StreamingOrchestrator.register_consumer(good)

        handle = StreamingOrchestrator.initialize_action("my_action", {})
        handle.new_data("data")
        handle.finish()

        assert len(good.received_data) == 1

    def test_consumer_fails_during_finish_others_still_finished(self):
        failing = FailingConsumer(["my_action"], fail_on="finish")
        good = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(failing)
        StreamingOrchestrator.register_consumer(good)

        handle = StreamingOrchestrator.initialize_action("my_action", {})
        handle.finish()

        assert good.finished_actions == ["my_action"]


class TestStreamingContextManager:
    def test_context_manager_calls_finish(self):
        consumer = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(consumer)

        with StreamingOrchestrator.initialize_action("my_action", {}) as handle:
            handle.new_data("data")

        assert consumer.finished_actions == ["my_action"]

    def test_context_manager_with_producer_exception(self):
        consumer = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(consumer)

        with pytest.raises(ValueError, match="producer error"):
            with StreamingOrchestrator.initialize_action("my_action", {}) as handle:
                handle.new_data("data")
                raise ValueError("producer error")

        # finish() was still called despite the exception
        assert consumer.finished_actions == ["my_action"]


class TestStreamingFinishIdempotent:
    def test_finish_is_idempotent(self):
        consumer = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(consumer)

        handle = StreamingOrchestrator.initialize_action("my_action", {})
        handle.finish()
        handle.finish()

        # finish called only once on consumer
        assert consumer.finished_actions == ["my_action"]


class TestStreamingReset:
    def test_reset_clears_state(self):
        consumer = MockConsumer(["my_action"])
        StreamingOrchestrator.register_consumer(consumer)
        assert len(StreamingOrchestrator._consumers) == 1

        StreamingOrchestrator.reset()
        assert len(StreamingOrchestrator._consumers) == 0
        assert len(StreamingOrchestrator._action_consumers) == 0

        # After reset, no consumers receive data
        handle = StreamingOrchestrator.initialize_action("my_action", {})
        handle.new_data("data")
        handle.finish()
        assert len(consumer.received_data) == 0


class TestStreamingProducer:
    def test_producer_validates_action(self):
        producer = MockProducer(["valid_action"])

        with pytest.raises(ValueError, match="does not produce action 'invalid_action'"):
            producer.initialize_action("invalid_action")

    def test_producer_delegates_to_orchestrator(self):
        consumer = MockConsumer(["valid_action"])
        StreamingOrchestrator.register_consumer(consumer)

        producer = MockProducer(["valid_action"])
        ctx = {"info": "test"}
        handle = producer.initialize_action("valid_action", ctx)

        assert isinstance(handle, StreamingActionHandle)
        assert len(consumer.initialized) == 1
        assert consumer.initialized[0] == ("valid_action", ctx)

        handle.new_data("producer_data")
        handle.finish()
        assert len(consumer.received_data) == 1
