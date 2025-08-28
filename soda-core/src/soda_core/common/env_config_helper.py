from __future__ import annotations

import logging
import os
from distutils.util import strtobool

from dotenv import load_dotenv
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


class EnvConfigHelper:
    """
    Helper class to manage environment configuration, feature flags and other config inputs like config files.

    Each attribute is a method for:
    - readibility and consistency
    - to allow for easy mocking in tests
    - to allow overriding of config from different sources (e.g., environment variables, config files) and other logic
    """

    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._initialize()
        return cls.__instance

    @classmethod
    def reset(cls):
        cls.__instance = None

    def _initialize(self):
        logger.debug("Loading environment variables from .env file.")
        load_dotenv(override=True)

    @property
    def soda_core_telemetry_enabled(self) -> bool:
        return strtobool(os.getenv("SODA_CORE_TELEMETRY_ENABLED", "true"))

    @property
    def soda_core_telemetry_local_debug_mode(self) -> bool:
        return strtobool(os.getenv("SODA_CORE_TELEMETRY_LOCAL_DEBUG_MODE", "false"))

    @property
    def soda_core_telemetry_test_mode(self) -> bool:
        return strtobool(os.getenv("SODA_CORE_TELEMETRY_TEST_MODE", "false"))
