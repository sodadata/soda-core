"""The dataset a check collection checks, while it builds its checks, verifies and post-processes.

Custom SQL names the checked dataset's table with ``${soda.DATASET}``. The parse leaves the slot as written. The
data source writes the table's name in when it runs the SQL, see ``DataSourceImpl.fill_soda_dataset``. SQL that
reaches a database some other way keeps the slot as text, and fails there.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator, Optional

SODA_DATASET: str = "${soda.DATASET}"


@dataclass(frozen=True)
class CheckedDataset:
    # The data source that runs the checks. A data standard may run on one named otherwise than its dataset's DQN.
    data_source_name: str
    prefixes: tuple[str, ...]
    dataset_name: str


_checked_dataset: ContextVar[Optional[CheckedDataset]] = ContextVar("soda_checked_dataset", default=None)


def get_checked_dataset() -> Optional[CheckedDataset]:
    return _checked_dataset.get()


@contextmanager
def using_checked_dataset(checked_dataset: CheckedDataset) -> Iterator[None]:
    token = _checked_dataset.set(checked_dataset)
    try:
        yield
    finally:
        _checked_dataset.reset(token)
