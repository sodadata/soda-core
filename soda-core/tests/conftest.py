# Order is important.  helpers.time_machine must be imported before helpers.test_fixtures to ensure that
# all imports datetime.datetime get the mockable TimeMachineDatetime
from helpers.a_time_machine import *
from helpers.test_fixtures import *
from soda_core.common.logging_configuration import configure_logging


def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)
