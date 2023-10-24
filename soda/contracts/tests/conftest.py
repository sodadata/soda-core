from __future__ import annotations

import logging
from typing import Any

from common.logs import configure_logging


# Load local env file so that test data sources can be set up.
from dotenv import load_dotenv

project_root_dir = __file__[: -len("soda/core/tests/helpers/fixtures.py")]
load_dotenv(f"{project_root_dir}/.env", override=True)


def pytest_sessionstart(session: Any) -> None:
    configure_logging()


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    """
    Prints the test function name and the location in a format that PyCharm recognizes and turns into a link in the console
    """
    logging.debug(f'### "soda/core/tests/{location[0]}:{(location[1] + 1)}" {location[2]}')
