#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


class FailedRowsProcessor:
    def process(self, context: dict):
        """
        Override this class in your program to process the failed rows
        :param context: dict with following keys:

            column_name : Name of the Column
            sample_name : Generated name of the sample
            sample_columns: Columns of the selected samples
            sample_rows: List of rows
            sample_description: Auto-generated description of the samples
            total_row_count: total count of the failed rows

        :return: dict with  message and count keys which will be sent to soda cloud instead of failed rows
        e.g. {'message': 'Failed Rows are saved in S3 bucket s3:///test_failed_rows'
             'count': 23}
        """
        pass
