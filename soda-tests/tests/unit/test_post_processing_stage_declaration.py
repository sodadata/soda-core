from unittest.mock import MagicMock

import pytest
from soda_core.common.scan_context import AtomicScanContext, BatchedScanContext, using_scan_context
from soda_core.contracts.contract_verification import PostProcessingStage, PostProcessingStageState
from soda_core.contracts.impl.contract_verification_impl import (
    ContractVerificationHandler,
    ContractVerificationHandlerRegistry,
    collect_post_processing_stages,
)


class _HandlerNeedingHandles(ContractVerificationHandler):
    """Stands in for the diagnostics-warehouse extractor: it can only run when the results
    insert returns the Cloud-minted ids."""

    requires_result_handles = True

    def handle(self, *args, **kwargs):
        pass

    def provides_post_processing_stages(self) -> list[PostProcessingStage]:
        return [PostProcessingStage("needsHandles", PostProcessingStageState.ONGOING)]


class _HandlerNotNeedingHandles(ContractVerificationHandler):
    def handle(self, *args, **kwargs):
        pass

    def provides_post_processing_stages(self) -> list[PostProcessingStage]:
        return [PostProcessingStage("selfContained", PostProcessingStageState.ONGOING)]


@pytest.fixture
def registered_handlers():
    original_handlers = list(ContractVerificationHandlerRegistry.contract_verification_handlers)
    original_stages = dict(ContractVerificationHandlerRegistry.post_processing_stages)
    ContractVerificationHandlerRegistry.contract_verification_handlers = []
    ContractVerificationHandlerRegistry.post_processing_stages = {}
    ContractVerificationHandlerRegistry.register(_HandlerNeedingHandles())
    ContractVerificationHandlerRegistry.register(_HandlerNotNeedingHandles())
    yield
    ContractVerificationHandlerRegistry.contract_verification_handlers = original_handlers
    ContractVerificationHandlerRegistry.post_processing_stages = original_stages


def _stage_names(stages: list[PostProcessingStage]) -> list[str]:
    return sorted(stage.name for stage in stages)


def test_atomic_run_provides_result_handles():
    assert AtomicScanContext(soda_cloud=MagicMock()).provides_result_handles is True


def test_batched_run_does_not_provide_result_handles():
    # A batch upload lands in object storage, so Soda Cloud returns no scan/dataset/check ids.
    assert BatchedScanContext(MagicMock(), scan_id="scan-123").provides_result_handles is False


def test_all_stages_are_declared_on_an_atomic_run(registered_handlers):
    with using_scan_context(AtomicScanContext(soda_cloud=MagicMock())):
        assert _stage_names(collect_post_processing_stages()) == ["needsHandles", "selfContained"]


def test_stages_needing_handles_are_not_declared_on_a_batched_run(registered_handlers):
    # Declaring a stage the run cannot complete leaves it ONGOING forever on Soda Cloud, which
    # also keeps the scan's logs pending server-side.
    with using_scan_context(BatchedScanContext(MagicMock(), scan_id="scan-123")):
        assert _stage_names(collect_post_processing_stages()) == ["selfContained"]
