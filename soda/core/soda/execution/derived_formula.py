from typing import Callable, Dict


class DerivedFormula:
    def __init__(self, function: Callable, metric_dependencies: Dict[str, "Metric"]):
        self.function: Callable = function
        self.metric_dependencies: Dict[str, "Metric"] = metric_dependencies
