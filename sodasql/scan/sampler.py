import logging
import re
from datetime import datetime
from time import strftime, gmtime

from sodasql.scan.metric import Metric
from sodasql.scan.samples_yml import SamplesYml
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan import Scan


class Sampler:

    def __init__(self, scan: Scan):
        self.scan = scan
        self.scan_reference = self.scan.scan_reference
        self.scan_folder_name = (f'{self._fileify(self.scan.warehouse.name)}' +
                                 f'-{self._fileify(self.scan.scan_yml.table_name)}' +
                                 (f'-{self.scan.time}' if isinstance(self.scan.time, str) else '') +
                                 (f'-{self.scan.time.strftime("%Y%m%d%H%M%S")}' if isinstance(self.scan.time, datetime) else '') +
                                 f'-{datetime.now().strftime("%Y%m%d%H%M%S")}')

    def _fileify(self, name: str):
        return re.sub(r'[^A-Za-z0-9_.]*', '', name).lower()

    def save_column_sql(self, samples_yml: SamplesYml, measurement):
        """
        Builds and executes SQL to save a sample for the given measurement
        Args:
            samples_yml: For column metric samples, samples_yml will have the defaults from the scan samples
                         already resolved.
        """
        column_name = measurement.column_name
        scan_column: ScanColumn = self.scan.scan_columns[column_name.lower()] if column_name else None

        # rows_total is the total number of rows from which the sample is taken
        rows_total = measurement.value
        sample_name = None
        where_clause = None
        tablesample = None
        limit = None

        if measurement.metric == Metric.ROW_COUNT:
            sample_name = 'dataset'
            tablesample = samples_yml.dataset_tablesample
            limit = samples_yml.dataset_limit
        elif measurement.metric == Metric.MISSING_COUNT:
            sample_name = 'missing'
            where_clause = scan_column.missing_condition
            tablesample = samples_yml.failed_tablesample
            limit = samples_yml.failed_limit
        elif measurement.metric == Metric.VALUES_COUNT:
            sample_name = 'values'
            where_clause = f'NOT ({scan_column.missing_condition})'
            tablesample = samples_yml.passed_tablesample
            limit = samples_yml.passed_limit
        elif measurement.metric == Metric.INVALID_COUNT:
            sample_name = 'invalid'
            where_clause = f'NOT ({scan_column.missing_condition}) AND NOT ({scan_column.valid_condition})'
            tablesample = samples_yml.failed_tablesample
            limit = samples_yml.failed_limit
        elif measurement.metric == Metric.VALID_COUNT:
            sample_name = 'valid'
            where_clause = f'NOT ({scan_column.missing_condition}) AND ({scan_column.valid_condition})'
            tablesample = samples_yml.passed_tablesample
            limit = samples_yml.passed_limit

        sql = (f"SELECT * \n"
               f"FROM {self.scan.qualified_table_name}")

        if where_clause:
            sql += f" \nWHERE {where_clause}"

        if tablesample is not None:
            sql += f" \nTABLESAMPLE {tablesample};"
        else:
            sql += f" \nLIMIT {limit};"

        file_path = (f'{self.scan_folder_name}/' +
                     (f'{self._fileify(column_name)}_' if column_name else '') +
                     f'{sample_name}.jsonl')

        sample_description = (f'{self.scan.scan_yml.table_name}.sample'
                              if sample_name == 'dataset' else f'{self.scan.scan_yml.table_name}.{column_name}.{sample_name}')

        logging.debug(f'Sampling {sample_description}')
        cursor = self.scan.warehouse.connection.cursor()
        try:
            logging.debug(f'Executing SQL query: \n{sql}')
            start = datetime.now()
            cursor.execute(sql)
            sample_columns = [
                {'name': d[0],
                 'type': self.scan.warehouse.dialect.get_type_name(d)}
                for d in cursor.description
            ]

            rows_stored, file_id = self.scan.soda_server_client.scan_upload(
                self.scan_reference,
                cursor,
                file_path)

            delta = datetime.now() - start
            logging.debug(f'SQL took {str(delta)}')

            self.scan.soda_server_client.scan_file(
                scan_reference=self.scan_reference,
                sample_type=sample_name + 'Sample',
                stored=int(rows_stored),
                total=int(rows_total),
                source_columns=sample_columns,
                file_path=file_path,
                file_id=file_id,
                column_name=column_name)

            logging.debug(f'Sent sample {sample_description} ({rows_stored}/{rows_total}) to Soda Cloud')
        except Exception:
            logging.exception(f'Sampling {sample_description} failed')
            raise
        finally:
            cursor.close()
