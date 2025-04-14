from soda_core.common.logging_configuration import configure_logging
from helpers.test_fixtures import *


def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)
