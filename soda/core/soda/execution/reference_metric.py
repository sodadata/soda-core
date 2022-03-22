from typing import Optional

from soda.execution.query_metric import QueryMetric
from soda.sampler.storage_ref import StorageRef


class ReferenceMetric(QueryMetric):
    def __init__(
        self,
        data_source_scan: "DataSourceScan",
        check: "ReferentialIntegrityCheck",
        partition: "Partition",
        single_source_column: "Column",
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            partition=partition,
            column=single_source_column,
            name="reference",
            check=check,
            identity_parts=[
                check.check_cfg.source_column_names,
                check.check_cfg.target_table_name,
                check.check_cfg.target_column_names,
            ],
        )
        self.check = check
        self.invalid_references_storage_ref: Optional[StorageRef] = None

    def __str__(self):
        return f'"{self.name}"'

    def ensure_query(self):
        from soda.execution.reference_query import ReferenceQuery

        self.data_source_scan.queries.append(ReferenceQuery(data_source_scan=self.data_source_scan, metric=self))
