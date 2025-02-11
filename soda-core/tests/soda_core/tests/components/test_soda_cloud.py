import os
from typing import Optional

from dotenv import load_dotenv

from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import YamlSource, YamlFileContent
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.mock_soda_cloud import MockSodaCloud, MockResponse
from soda_core.tests.helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("soda_cloud")
    .column_text("id")
    .column_integer("age")
    .rows(rows=[
        ("1",  1),
        (None, -1),
        ("3",  None),
        ("X",  2),
    ])
    .build()
)


def test_soda_cloud_results(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    load_dotenv(".env", override=True)

    soda_cloud: Optional[SodaCloud] = None

    if os.environ.get("TEST_ON_REAL_SODA_CLOUD") == "true":
        logs: Logs = Logs()
        yaml_file_content: YamlFileContent = YamlSource.from_dict({}).parse_yaml_file_content(logs=logs)
        SodaCloud.from_file(soda_cloud_file_content=yaml_file_content)
        if logs.has_errors():
            raise AssertionError(str(logs))
    else:
        soda_cloud = MockSodaCloud(responses=[MockResponse(
            status_code=200,
            json_dict={"fileId": "777ggg"}
        )])

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [-1, -2]
                checks:
                  - type: missing_count
                    must_be_between: [1, 3]
        """,
        soda_cloud=soda_cloud
    )
