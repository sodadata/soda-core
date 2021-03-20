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
                    'valid_format': 'number_whole'
                },
                'date': {
                    'missing_values': ['N/A', 'No value']
                }
            }
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

    def get_file_data(self, sample_type: str, column_name=None):
        command = next(command
                       for command in self.mock_soda_server_client.commands
                       if (command['type'] == 'scannerScanFile'
                           and command['sampleType'] == sample_type
                           and command.get('columnName') == column_name))
        return self.mock_soda_server_client.file_uploads[command['fileId']]['data']
