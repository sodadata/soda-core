from tests.common.sql_test_case import SqlTestCase


class TestSamplesAndFailedRows(SqlTestCase):

    def test_samples_and_failed_rows(self):
        self.use_mock_soda_server_client()

        self.sql_recreate_table([
            "id VARCHAR(255)",
            "date VARCHAR(255)"
        ], [
            "( '1', 'N/A')",
            "( '2', null)",
            "( 'x', '2021-01-23')",
            "( 'x', '2021-01-24')"
        ])

        self.scan({
            'metric_groups': ['missing', 'validity'],
            'samples': {
                'table_limit': 10,
                'failed_limit': 5
            },
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
                    'sql': 'SELECT * FROM %s WHERE true = false' % (self.default_test_table_name)
                }
            ]
        })

        dataset_sample_file_contents = self.get_file_data(sample_type='datasetSample')
        self.assertEqual(dataset_sample_file_contents,
                         '["1", "N/A"]\n'
                         '["2", null]\n'
                         '["x", "2021-01-23"]\n'
                         '["x", "2021-01-24"]\n')

        id_invalid_sample_file_contents = self.get_file_data(sample_type='invalidSample', column_name='id')
        self.assertEqual(id_invalid_sample_file_contents,
                         '["x", "2021-01-23"]\n'
                         '["x", "2021-01-24"]\n')

        date_missing_sample_file_contents = self.get_file_data(sample_type='missingSample', column_name='date')
        self.assertEqual(date_missing_sample_file_contents,
                         '["1", "N/A"]\n'
                         '["2", null]\n')

        test_results_command = next(command for command in self.mock_soda_server_client.commands
                                    if (command['type'] == 'sodaSqlScanTestResults'))

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
                'table_limit': 5,
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
                    'name': 'X_TEST',
                    'sql': 'SELECT * FROM %s WHERE id = \'x\'' % (self.default_test_table_name)
                }
            ]
        })

        dataset_sample_file_contents = self.get_file_data(sample_type='datasetSample')
        self.assertEqual(len(dataset_sample_file_contents.splitlines()), 5)

        id_invalid_sample_file_contents = self.get_file_data(sample_type='invalidSample', column_name='id')
        self.assertEqual(len(id_invalid_sample_file_contents.splitlines()), 5)

        failed_rows_file_contents = self.get_file_data(sample_type='failedRowsSample', sql_metric_name='X_TEST')
        self.assertEqual(len(failed_rows_file_contents.splitlines()), 5)

        test_results_command = next(command for command in self.mock_soda_server_client.commands
                                    if (command['type'] == 'sodaSqlScanTestResults'))


    def get_file_data(self, sample_type: str, column_name=None, sql_metric_name=None):
        command = next(command
                       for command in self.mock_soda_server_client.commands
                       if (command['type'] == 'scannerScanFile'
                           and command['sampleType'] == sample_type
                           and command.get('columnName') == column_name
                           and command.get('sqlMetricName') == sql_metric_name))
        return self.mock_soda_server_client.file_uploads[command['fileId']]['data']
