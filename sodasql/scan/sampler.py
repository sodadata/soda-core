import logging
import re
from time import strftime, gmtime

from sodasql.scan.metric import Metric
from sodasql.scan.samples_yml import SamplesYml
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan import Scan


class Sampler:

    def __init__(self, scan: Scan):
        self.scan = scan
        self.scan_reference = self.scan.scan_reference
        self.scan_folder_name = f'{self._fileify(self.scan.warehouse.name)}-' \
                                f'{self._fileify(self.scan.scan_yml.table_name)}-' \
                                f'{self.scan.time}-{strftime("%Y%m%d%H%M%S", gmtime())}'

    def _fileify(self, name: str):
        return re.sub(r'[^A-Za-z0-9_.]*', '', name).lower()

    def save_column_sql(self, column_samples_yml: SamplesYml, measurement):
        column_name = measurement.column_name

        scan_column: ScanColumn = self.scan.scan_columns[column_name.lower()]
        sample_name = 'missing'
        condition = scan_column.missing_condition
        total_failed_row_count = measurement.value
        tablesample = None
        limit = None

        if measurement.metric == Metric.MISSING_COUNT:
            sample_name = 'missing'
            condition = scan_column.missing_condition
            tablesample = column_samples_yml.failed_tablesample
            limit = column_samples_yml.failed_limit
        elif measurement.metric == Metric.VALUES_COUNT:
            sample_name = 'values'
            condition = f'NOT ({scan_column.missing_condition})'
            tablesample = column_samples_yml.passed_tablesample
            limit = column_samples_yml.passed_limit
        elif measurement.metric == Metric.INVALID_COUNT:
            sample_name = 'invalid'
            condition = f'NOT ({scan_column.missing_condition}) AND NOT ({scan_column.valid_condition})'
            tablesample = column_samples_yml.failed_tablesample
            limit = column_samples_yml.failed_limit
        elif measurement.metric == Metric.VALID_COUNT:
            sample_name = 'valid'
            condition = f'NOT ({scan_column.missing_condition}) AND ({scan_column.valid_condition})'
            tablesample = column_samples_yml.passed_tablesample
            limit = column_samples_yml.passed_limit

        sql = (f"SELECT * \n"
               f"FROM {self.scan.qualified_table_name} \n"
               f"WHERE {condition} \n")

        if tablesample is not None:
            sql += f"TABLESAMPLE {tablesample};"
        else:
            sql += f"LIMIT {limit};"

        file_path = \
            f'{self.scan_folder_name}' \
            f'/{self._fileify(column_name)}' \
            f'-{sample_name}.jsonl'

        logging.debug(f'Saving {sample_name} of {column_name} in {file_path} using {sql}')
        cursor = self.scan.warehouse.connection.cursor()
        try:
            cursor.execute(sql)
            sample_columns = self.get_sample_columns(cursor)

            stored_failed_rows, file_id = self.scan.soda_server_client.scan_upload(
                self.scan_reference,
                cursor,
                file_path)

            self.scan.soda_server_client.scan_file(
                scan_reference=self.scan_reference,
                sample_type=sample_name + 'Sample',
                stored=int(stored_failed_rows),
                total=int(total_failed_row_count),
                source_columns=sample_columns,
                file_path=file_path,
                file_id=file_id,
                column_name=column_name)
            logging.debug(f'Save {sample_name} of {column_name} ok')
        except Exception:
            logging.exception(f'Saving {sample_name} of {column_name}')
            raise
        finally:
            cursor.close()

    def save_sample(self, samples_yml: SamplesYml, row_count: int):
        table_name: str = self.scan.scan_yml.table_name

        file_path = \
            f'{self.scan_folder_name}' \
            f'/sample.jsonl'

        sql = (f"SELECT * \n"
               f"FROM {self.scan.qualified_table_name} \n")

        if samples_yml.dataset_tablesample is not None:
            sql += f"TABLESAMPLE {samples_yml.dataset_tablesample};"
        else:
            sql += f"LIMIT {samples_yml.dataset_limit};"

        logging.debug(f'Saving sample of {table_name} in {file_path} using {sql}')

        cursor = self.scan.warehouse.connection.cursor()
        try:
            cursor.execute(sql)

            sample_columns = self.get_sample_columns(cursor)

            stored_sample_rows, file_id = self.scan.soda_server_client.scan_upload(
                scan_reference=self.scan_reference,
                cursor=cursor,
                file_path=file_path)

            self.scan.soda_server_client.scan_file(
                scan_reference=self.scan_reference,
                sample_type='datasetSample',
                stored=int(stored_sample_rows),
                total=int(row_count),
                source_columns=sample_columns,
                file_path=file_path,
                file_id=file_id)

            logging.debug(f'Save sample file {file_path} for table {table_name} ok')
        except Exception:
            logging.exception(f'Save sample file {file_path} for table {table_name} failed!')
            raise
        finally:
            cursor.close()

    def get_sample_columns(self, cursor):
        return [{'name': d[0], 'type': self.scan.warehouse.dialect.get_type_name(d)} for d in cursor.description]

