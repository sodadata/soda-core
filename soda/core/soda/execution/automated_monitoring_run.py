from typing import Dict, List

from soda.anomaly_detection.anomaly_detector import AnomalyInput, AnomalyOutput
from soda.execution.automated_monitoring_result import AutomatedMonitoringResult
from soda.execution.data_source_scan import DataSourceScan
from soda.execution.query import Query
from soda.execution.schema_comparator import SchemaComparator
from soda.sodacl.automated_monitoring_cfg import AutomatedMonitoringCfg


class AutomatedMonitoringRun:
    def __init__(self, data_source_scan: DataSourceScan, automated_monitoring_cfg: AutomatedMonitoringCfg):
        self.data_source_scan: DataSourceScan = data_source_scan
        self.soda_cloud = data_source_scan.scan._configuration.soda_cloud
        self.data_source = data_source_scan.data_source
        self.automated_monitoring_cfg: AutomatedMonitoringCfg = automated_monitoring_cfg
        self.field_tablename = '"tablename"'
        self.logs = self.data_source_scan.scan._logs

    def run(self) -> AutomatedMonitoringResult:
        automated_monitoring_result: AutomatedMonitoringResult = AutomatedMonitoringResult(
            self.automated_monitoring_cfg
        )

        if self.automated_monitoring_cfg.row_count:
            # row_counts is a dict that maps table names to row counts.
            row_counts_by_table_name: Dict[str, int] = self.get_row_counts_for_all_tables()
            for measured_table_name in row_counts_by_table_name:
                measured_row_count = row_counts_by_table_name[measured_table_name]
                anomaly_input: AnomalyInput = self.get_historic_row_count_anomaly_input_from_soda_cloud(
                    measured_table_name
                )
                anomaly_output: AnomalyOutput = self.evaluate_anomaly(anomaly_input=anomaly_input)
                automated_monitoring_result.append_row_count_anomaly_evaluation_result(
                    table_name=measured_table_name, anomaly_output=anomaly_output
                )

        if self.automated_monitoring_cfg.schema:
            # {table_name -> {column_name -> column_type}}
            measured_columns_by_table_name: Dict[str, Dict[str, str]] = self.get_columns_for_all_tables()
            historic_schema_by_table_name = self.get_historic_schema_by_table_from_soda_cloud()

            tables_added = set()
            tables_removed = {historic_table_name for historic_table_name in historic_schema_by_table_name.keys()}

            for measured_table_name in measured_columns_by_table_name:
                tables_removed.discard(measured_table_name)
                if measured_table_name not in historic_schema_by_table_name:
                    tables_added.add(measured_table_name)

                measured_table_schema = measured_columns_by_table_name[measured_table_name]
                historic_table_schema = historic_schema_by_table_name.get(measured_table_name)

                if historic_table_schema is not None:
                    schema_comparator = SchemaComparator(
                        historic_schema=historic_table_schema, measured_schema=measured_table_schema
                    )

                    automated_monitoring_result.append_table_schema_changes(schema_comparator)
                else:
                    self.logs.debug("No schema auto monitoring because there is not previous schema info")

            automated_monitoring_result.append_table_changes(list(tables_added), list(tables_removed))

        return automated_monitoring_result

    def get_row_counts_for_all_tables(self) -> Dict[str, int]:
        """
        Returns a dict that maps table names to row counts.
        Later this could be implemented with different queries depending on the data source type.
        """
        include_tables = self.automated_monitoring_cfg.include_tables
        exclude_tables = self.automated_monitoring_cfg.exclude_tables
        sql = self.data_source.sql_get_table_names_with_count(
            include_tables=include_tables, exclude_tables=exclude_tables
        )
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name="get_counts_by_tables_for_row_count_anomalies",
            sql=sql,
        )
        query.execute()
        return {row[0]: row[1] for row in query.rows}

    def get_columns_for_all_tables(self) -> Dict[str, Dict[str, str]]:
        """
        Returns a dict that maps table names to a dict that maps column names to column types.
        {table_name -> {column_name -> column_type}}
        """
        include_tables = self.automated_monitoring_cfg.include_tables
        exclude_tables = self.automated_monitoring_cfg.exclude_tables
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
