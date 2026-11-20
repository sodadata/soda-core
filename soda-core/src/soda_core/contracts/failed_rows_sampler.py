from abc import ABC, abstractmethod

from soda_core.contracts.contract_verification import CheckResult


class FailedRowsSampler(ABC):
    """Sink for failed-row sample data.

    Sample limits are resolved by the execution engine from Soda Cloud's
    dataset row-sampler configuration. They are intentionally not part of
    this callback API and are currently scoped to the enclosing contract,
    rather than individual failed-rows checks.
    """

    @abstractmethod
    def process_sample(self, rows: list[dict], columns: list[str], check_result: CheckResult) -> None:
        pass
