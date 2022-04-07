from typing import Dict, List

from soda.anomaly_detection.anomaly_detector import AnomalyInput, AnomalyOutput
from soda.execution.profile_columns_result import ProfileColumnsResult
from soda.execution.query import Query
from soda.sodacl.profile_columns_cfg import ProfileColumnsCfg


class ProfileColumnsRun:

    def __init__(self, data_source_scan: "DataSourceScan", profile_columns_cfg: ProfileColumnsCfg):
        from soda.execution.data_source_scan import DataSourceScan
        self.data_source_scan: DataSourceScan = data_source_scan
        self.soda_cloud = data_source_scan.scan._configuration.soda_cloud
        self.data_source = data_source_scan.data_source
        self.profile_columns_cfg: ProfileColumnsCfg = profile_columns_cfg
        self.logs = self.data_source_scan.scan._logs

    def run(self) -> ProfileColumnsResult:
        profile_columns_result: ProfileColumnsResult = ProfileColumnsResult(
            self.profile_columns_cfg
        )

        # row_counts is a dict that maps table names to row counts.
        row_counts_by_table_name: Dict[str, int] = self.get_row_counts_for_all_tables()
        for measured_table_name in row_counts_by_table_name:
            measured_row_count = row_counts_by_table_name[measured_table_name]
            profile_columns_result_table = profile_columns_result.create_table(measured_table_name, measured_row_count)

        return profile_columns_result

    def get_row_counts_for_all_tables(self) -> Dict[str, int]:
        """
        Returns a dict that maps table names to row counts.
        Later this could be implemented with different queries depending on the data source type.
        """
        include_tables = []
        include_tables.extend(self._get_table_expression(self.profile_columns_cfg.include_columns))
        include_tables.extend(self._get_table_expression(self.profile_columns_cfg.exclude_columns))
        sql = self.data_source.sql_get_table_names_with_count(
            include_tables=include_tables
        )
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name="get_counts_by_tables_for_profile_columns",
            sql=sql,
        )
        query.execute()
        return {row[0]: row[1] for row in query.rows}

    def _get_table_expression(self, include_columns: List[str]) -> List[str]:
        if len(include_columns) == 0:
            return ['%']
        table_expressions = []
        for include_column_expression in include_columns:
            parts = include_column_expression.split('.')
            if len(parts) != 2:
                self.logs.error(f'Invalid include column expression "{include_column_expression}"', location=self.profile_columns_cfg.location)
            else:
                table_expression = parts[0]
                table_expressions.append(table_expression)
        return table_expressions

    def get_columns_for_all_tables(self) -> Dict[str, Dict[str, str]]:
        """
        Returns a dict that maps table names to a dict that maps column names to column types.
        {table_name -> {column_name -> column_type}}
        """
        include_tables = self.profile_columns_cfg.include_tables
        exclude_tables = self.profile_columns_cfg.exclude_tables
        sql = self.data_source.sql_get_column(include_tables=include_tables, exclude_tables=exclude_tables)
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name="get_counts_by_tables_for_row_count_anomalies",
            sql=sql,
        )
        query.execute()

        columns_by_table_name: Dict[str, Dict[str, str]] = {}

        for row in query.rows:
            columns_by_table_name.setdefault(row[0], {})[row[1]] = row[2]

        return columns_by_table_name

    def get_historic_row_count_anomaly_input_from_soda_cloud(self, table_name: str) -> AnomalyInput:
        data_source_name = self.data_source_scan.data_source.data_source_name
        historic_query = {
            "gimme": "historic row count measurements",
            "and also": "the check results with feedback",
            "for data source": data_source_name,
            "and table": table_name,
        }
        soda_cloud_response = self.soda_cloud.get(historic_query)
        timed_values = []  # Extract timed values from soda_cloud_response (or multiple responses if needed)
        return AnomalyInput(timed_values=timed_values)

    def get_historic_schema_by_table_from_soda_cloud(self) -> Dict[str, List[List[object]]]:
        data_source_name = self.data_source_scan.data_source.data_source_name
        """
        Gets the previous schema for all tables for this automated monitoring configuration from Soda Cloud
        {table_name -> [[column_name, column_type], [column_name, column_type], ...]}
        """
        historic_query = {
            "gimme": "all previous table schemas",
            "measured": "previously",
            "for automated monitoring configuration in data source": data_source_name
            # TODO will we allow 2 automated monitoring configs for a single data source? If so, how do we distinct them?
        }
        soda_cloud_response = self.soda_cloud.get(historic_query)
        extracted_historic_schemas = {}
        return extracted_historic_schemas

    def evaluate_anomaly(self, anomaly_input: AnomalyInput) -> AnomalyOutput:
        # TODO delegate to AnomalyDetector
        return AnomalyOutput(is_anomaly=False, anomaly_score=0.45)
