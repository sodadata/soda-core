from __future__ import annotations

import os
from distutils.util import strtobool

from dotenv import load_dotenv
from soda.common.logs import Logs


class EnvHelper:
    __instance = None

    def __new__(cls, logs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._initialize(logs)
        return cls.__instance

    @classmethod
    def reset(cls):
        cls.__instance = None

    def _initialize(self, logs):
        self.logs: Logs = logs

        self.logs.debug("Loading environment variables from .env file.")
        load_dotenv(override=True)

        self.ff_profiling_profiling_observability = strtobool(
            os.environ.get("SODA_FEATURE_FLAG_PROFILING_OBSERVABILITY", "false")
        )

        self.ff_chunked_scan_ingestion = strtobool(os.environ.get("SODA_FEATURE_FLAG_CHUNKED_SCAN_INGESTION", "false"))

        hard_limit = os.environ.get("SODA_QUERY_CURSOR_HARD_LIMIT", None)
        self.query_cursor_hard_limit: int | None = int(hard_limit) if hard_limit is not None else None

        self.ff_jinja_resolve_custom_identity = strtobool(
            os.environ.get("SODA_FEATURE_JINJA_RESOLVE_CUSTOM_IDENTITY", "false")
        )
