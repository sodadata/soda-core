from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FailedRowsStorageStrategy(Enum):
    STORE_FULL_ROWS = "store_full_rows"
    STORE_KEYS = "store_keys"


@dataclass
class DiagnosticsConfiguration:
    """
    All the failed rows diagnostics configurations apart from the
    data source configuration file for a single contract
    """

    # dwh_prefixes includes schema name
    dwh_prefixes: list[str]
    key_columns: list[str]
    strategy: FailedRowsStorageStrategy
    max_row_count: Optional[int]
