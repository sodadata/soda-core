import os

import freezegun
import pytest
from helpers.test_fixtures import *  # noqa: F401
from soda_core.common.logging_configuration import configure_logging
from soda_core.contracts.impl.contract_verification_impl import (
    ContractVerificationHandlerRegistry,
)


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers",
        "no_snapshot: mark test to be skipped when running in snapshot record/replay mode. "
        "Use @pytest.mark.no_snapshot to skip both modes, or "
        "@pytest.mark.no_snapshot(mode='replay') to skip only replay.",
    )
    config.addinivalue_line(
        "markers",
        "nightly_only: mark test to run only in nightly builds (implies no_snapshot). "
        "Skipped unless SODA_NIGHTLY=true is set.",
    )


def pytest_runtest_setup(item):
    if any(item.iter_markers(name="nightly_only")):
        if os.environ.get("SODA_NIGHTLY", "").lower() != "true":
            pytest.skip("Test is marked nightly_only and SODA_NIGHTLY is not set")

    for marker in item.iter_markers(name="no_snapshot"):
        snapshot_mode = os.environ.get("SODA_TEST_SNAPSHOT", "off").lower()
        restricted_mode = marker.kwargs.get("mode")
        if restricted_mode:
            if snapshot_mode == restricted_mode:
                pytest.skip(f"Test is marked no_snapshot and snapshot mode '{snapshot_mode}' is active")
        else:
            if snapshot_mode in ("record", "replay"):
                pytest.skip(f"Test is marked no_snapshot and snapshot mode '{snapshot_mode}' is active")


def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)


def monkey_patch_freezegun():
    def utcnow():
        if freezegun.api._should_use_real_time():
            result = freezegun.api.real_datetime.utcnow()
        else:
            result = freezegun.api.FakeDatetime._time_to_freeze()
        return freezegun.api.datetime_to_fakedatetime(result)

    def now(tz=None):
        if freezegun.api._should_use_real_time():
            result = freezegun.api.real_datetime.now(tz=tz)
        else:
            result = freezegun.api.FakeDatetime._time_to_freeze()
        return freezegun.api.datetime_to_fakedatetime(result)

    setattr(freezegun.api.FakeDatetime, "utcnow", utcnow)
    setattr(freezegun.api.FakeDatetime, "now", now)


monkey_patch_freezegun()


my_ignore_list = [
    "pyathena",
    "boto3",
    "botocore",
    "boto3",
    "botocore",
    "botocore.auth",
    "botocore.credentials",
    "botocore.awsrequest",
]
freezegun.configure(extend_ignore_list=my_ignore_list)
freeze_time = freezegun.freeze_time


@pytest.fixture(autouse=True)
def clear_contract_verification_handlers():
    ContractVerificationHandlerRegistry.contract_verification_handlers.clear()
    ContractVerificationHandlerRegistry.post_processing_stages.clear()
    yield
    ContractVerificationHandlerRegistry.contract_verification_handlers.clear()
    ContractVerificationHandlerRegistry.post_processing_stages.clear()
