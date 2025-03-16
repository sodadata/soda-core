from soda_core.common.logging_configuration import configure_logging
from soda_core.tests.helpers.test_fixtures import *


def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)
