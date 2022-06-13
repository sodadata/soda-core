from cmath import inf
from soda.execution.metric import Metric
from soda.execution.check import Check
from soda.execution.data_source_scan import DataSourceScan

class DistributionCheckMetric(Metric):
    def __init__(
        self,
        data_source_scan: DataSourceScan,
        check_name: str,
        check: Check = None,
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            partition=None,
            column=None,
            name=check_name,
            check=check,
            identity_parts=['fake_metric'],
        )
        self.value: object = inf
