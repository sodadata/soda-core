from datetime import datetime
from unittest import skip

from soda_core.common.soda_cloud import SodaCloud
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.mock_soda_cloud import MockResponse, MockHttpMethod
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

    data_source_test_helper.enable_soda_cloud_mock([MockResponse(
        status_code=200,
        json_object={"fileId": "777ggg"}
    )])

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [-1, -2]
                checks:
                  - type: missing_count
                    must_be_less_than_or_equal: 2
        """
    )


def test_execute_over_agent(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock([
        MockResponse(
            method=MockHttpMethod.POST,
            status_code=200,
            json_object={"fileId": "fffileid"}
        ),
        MockResponse(
            method=MockHttpMethod.POST,
            status_code=200,
            json_object={"scanId": "ssscanid"}
        ),
        MockResponse(
            method=MockHttpMethod.GET,
            status_code=200,
            headers={
                "X-Soda-Next-Poll-Time": SodaCloud.convert_datetime_to_str(
                    datetime.now()
                )
            },
            json_object={
                "scanId": "ssscanid",
                "state": "running"
            }
        ),
        MockResponse(
            method=MockHttpMethod.GET,
            status_code=200,
            json_object={
                "scanId": "ssscanid",
                "state": "completed"
            }
        ),
        MockResponse(
            method=MockHttpMethod.GET,
            status_code=200,
            json_object={
                "content": [
                    {
                        "level": "debug",
                        "message": "m1",
                        "timestamp": "2025-02-21T06:16:58+00:00",
                        "index": 0,
                    },
                    {
                        "level": "info",
                        "message": "m2",
                        "timestamp": "2025-02-21T06:16:59+00:00",
                        "index": 1,
                    }
                ],
                "totalElements": 2,
                "totalPages": 1,
                "number": 0,
                "size": 2,
                "last": True,
                "first": True
            }
        )
    ])

    data_source_test_helper.use_agent = True

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [-1, -2]
                checks:
                  - type: missing_count
                    must_be_less_than_or_equal: 2
        """
    )
