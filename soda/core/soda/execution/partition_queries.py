from typing import List, Optional

from soda.execution.aggregation_query import AggregationQuery
from soda.execution.duplicates_query import DuplicatesQuery
from soda.execution.numeric_query_metric import NumericQueryMetric
from soda.execution.partition import Partition
from soda.execution.query import Query
from soda.execution.schema_metric import SchemaMetric
from soda.execution.schema_query import SchemaQuery


class PartitionQueries:
    def __init__(self, partition: Partition):
        from soda.execution.data_source_scan import DataSourceScan

        self.logs = partition.data_source_scan.scan._logs
        self.partition: Partition = partition
        self.data_source_scan: DataSourceScan = partition.data_source_scan
        self.schema_query: Optional[SchemaQuery] = None
        self.aggregation_queries: List[AggregationQuery] = []
        self.duplicate_queries: List[DuplicatesQuery] = []

    def add_metric(self, metric: "Metric"):

        if isinstance(metric, SchemaMetric):
            self.schema_query = SchemaQuery(self.partition, metric)

        elif isinstance(metric, NumericQueryMetric):
            sql_aggregation_expression = metric.get_sql_aggregation_expression()
            if sql_aggregation_expression:
                max_aggregation_fields = self.data_source_scan.data_source.get_max_aggregation_fields()
                if (
                    len(self.aggregation_queries) == 0
                    or len(self.aggregation_queries[-1].metrics) >= max_aggregation_fields
                ):
                    aggregation_query_index = len(self.aggregation_queries)
                    aggregation_query = AggregationQuery(self.partition, aggregation_query_index)
                    self.aggregation_queries.append(aggregation_query)
                else:
                    aggregation_query = self.aggregation_queries[-1]
                aggregation_query.add_metric(sql_aggregation_expression, metric)

            elif metric.name == "duplicate_count":
                self.duplicate_queries.append(DuplicatesQuery(self.partition, metric))

            else:
                self.logs.error(f"Unsupported metric {metric.name}")
        else:
            self.logs.error(f"Unsupported metric {metric.name} ({type(metric).__name__})")

    def get_queries(self) -> List[Query]:
        queries: List[Query] = []
        if self.schema_query:
            queries.append(self.schema_query)
        queries.extend(self.aggregation_queries)
        queries.extend(self.duplicate_queries)
        return queries
