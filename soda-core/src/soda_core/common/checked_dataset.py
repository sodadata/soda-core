"""The dataset a check collection checks and the dataset of the same data source its queries read, its execution
dataset. Without the verify call's ``execution_dataset`` the two are one. A collection reads through
``execution_dataset`` and names the dataset by ``dataset`` everywhere else: check identities, Soda Cloud and the
diagnostics warehouse. Custom SQL that names a table reads that table.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import InvalidArgumentException

EXECUTION_DATASET_FORM: str = (
    "execution_dataset is a dataset of the contract's data source, written as its DQN without the data source, such "
    "as 'orders' or 'main/orders', with no empty part"
)


@dataclass(frozen=True)
class CheckedDataset:
    # The contract's dataset.
    dataset: DatasetIdentifier
    # The dataset of the same data source the run reads it from. Without execution_dataset, the dataset itself.
    execution_dataset: DatasetIdentifier

    @property
    def reads_another_dataset(self) -> bool:
        """Whether the run reads the checked dataset from another dataset, which is what an override is."""
        return not self.dataset.is_same_as(self.execution_dataset)


def parse_execution_dataset(dataset: DatasetIdentifier, execution_dataset: Optional[str]) -> DatasetIdentifier:
    """The dataset of ``dataset``'s data source that ``execution_dataset`` names, or ``dataset`` itself without one."""
    if execution_dataset is None:
        return dataset
    if not isinstance(execution_dataset, str) or "" in execution_dataset.split("/"):
        raise InvalidArgumentException(f"{EXECUTION_DATASET_FORM}. Got {execution_dataset!r}")
    return DatasetIdentifier.parse(f"{dataset.data_source_name}/{execution_dataset}")
