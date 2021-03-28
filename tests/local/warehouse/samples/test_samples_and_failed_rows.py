from tests.common.sql_test_case import SqlTestCase


class TestSamplesAndFailedRows(SqlTestCase):

    def test_samples_and_failed_rows_defaults(self):
        self.use_mock_soda_server_client()

        self.sql_recreate_table([
            "id VARCHAR(255)",
            "date VARCHAR(255)"
        ], [
            "( '1', 'N/A')",
            "( '2', null)",
            "( '2', null)",
            "( '2', null)",
            "( '2', null)",
            "( 'x', '2021-01-23')",
            "( 'x', '2021-01-24')"
        ])

        self.scan({
            'metric_groups': ['missing', 'validity'],
            'columns': {
                'id': {
                    'valid_format': 'number_whole',
                    'tests': [
                        'invalid_count == 0'
                    ]
                },
                'date': {
                    'missing_values': ['N/A', 'No value']
                }
            },
            'sql_metrics': [
                {
                    'type': 'failed_rows',
                    'name': 'X_TEST',
                    'sql': f'SELECT * FROM {self.default_test_table_name}'
                }
            ]
        })

        # By default, no table samples and no default metric samples
        self.assertIsNone(self.find_scan_file_command(sample_type='datasetSample'))
        self.assertIsNone(self.find_scan_file_command(sample_type='invalidSample', column_name='id'))
        self.assertIsNone(self.find_scan_file_command(sample_type='missingSample', column_name='date'))

        # By default for sql metrics type failed rows, 5 rows are sampled
        failed_rows_file_contents = self.get_file_data(sample_type='failedRowsSample', sql_metric_name='X_TEST')
        self.assertEqual(len(failed_rows_file_contents.splitlines()), 5)

    def test_samples_and_failed_rows_limits(self):
        self.use_mock_soda_server_client()

        self.sql_recreate_table([
            "id VARCHAR(255)",
            "date VARCHAR(255)"
        ], [
            "( '1', 'N/A')",
            "( '2', null)",
            "( 'x', '2021-01-23')",
            "( 'x', '2021-01-24')",
            "( 'x', '2021-01-25')",
            "( 'x', '2021-02-24')",
            "( 'x', '2021-03-24')",
            "( 'x', '2021-04-24')",
            "( 'x', '2021-05-10')",
            "( '10', '2021-05-11')",
        ])

        self.scan({
            'metric_groups': ['missing', 'validity'],
            'samples': {
                'table_limit': 8,
                'failed_limit': 5
            },
            'columns': {
                'id': {
                    'valid_format': 'number_whole',
                    'tests': [
                        'invalid_count == 0'
                    ]
                }
            },
            'sql_metrics': [
                {
                    'type': 'failed_rows',
                    'name': 'bad_row_count',
                    'sql': f'SELECT * FROM {self.default_test_table_name}',
                    'failed_limit': 3
                }
            ]
        })

        dataset_sample_file_contents = self.get_file_data(sample_type='datasetSample')
        self.assertEqual(len(dataset_sample_file_contents.splitlines()), 8)

        id_invalid_sample_file_contents = self.get_file_data(sample_type='invalidSample', column_name='id')
        self.assertEqual(len(id_invalid_sample_file_contents.splitlines()), 5)

        failed_rows_file_contents = self.get_file_data(sample_type='failedRowsSample', sql_metric_name='bad_row_count')
        self.assertEqual(len(failed_rows_file_contents.splitlines()), 3)

        test_results_command = next(command for command in self.mock_soda_server_client.commands
                                    if (command['type'] == 'sodaSqlScanTestResults'))

        self.assertFalse(test_results_command['testResults'][0]['passed'])
        self.assertEqual(3, test_results_command['testResults'][0]['values']['bad_row_count'])

    def get_file_data(self, sample_type: str, column_name=None, sql_metric_name=None):
        command = self.find_scan_file_command(sample_type, column_name, sql_metric_name)
        self.assertIsNotNone(command, f"Can't find scan file command for sample_type={sample_type} column_name={column_name} sql_metric_name={sql_metric_name}")
        return self.mock_soda_server_client.file_uploads[command['fileId']]['data']

    def find_scan_file_command(self, sample_type, column_name=None, sql_metric_name=None):
        return next((command for command in self.mock_soda_server_client.commands
                             if (command['type'] == 'sodaSqlScanFile'
                                 and command['sampleType'] == sample_type
                                 and command.get('columnName') == column_name
                                 and command.get('sqlMetricName') == sql_metric_name)),
                    None)
