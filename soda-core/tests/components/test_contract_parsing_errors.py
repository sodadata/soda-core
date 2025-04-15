import pytest

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import dedent_and_strip
from soda_core.common.exceptions import InvalidContractException, InvalidDatasetQualifiedNameException


def test_parsing_error_wrong_type(data_source_test_helper: DataSourceTestHelper):
    with pytest.raises(InvalidDatasetQualifiedNameException):
        data_source_test_helper.assert_contract_error(
            dedent_and_strip(
                """
                dataset:
                  data_source: milan_nyc
                  namespace_prefix: [soda_test, dev_tom]
                  name: SODATEST_soda_cloud_11c53cd6

                columns:
                  - name: id
                    valid_values: ['1', '2', '3']
                    checks:
                      - invalid:
                  - name: age
                checks:
                  - schema:
        """
            )
        )


def test_duplicate_identity_error(data_source_test_helper: DataSourceTestHelper):
    errors_str: str = data_source_test_helper.assert_contract_error(
        contract_yaml_str="""
            dataset: the_test_ds/a/b/TBLE
            columns:
              - name: id
                checks:
                  - missing:
                  - missing:
                      name: lksdfj
        """
    )

    assert (
        "Duplicate identity yaml_string.yml/id/missing. Original(yaml_string.yml[4,8]) Duplicate(yaml_string.yml[6,10])"
    ) in errors_str


def test_error_duplicate_column_names(data_source_test_helper: DataSourceTestHelper):
    errors_str: str = data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            dataset: the_test_ds/a/b/TBLE
            columns:
              - name: id
              - name: id
        """
    )

    assert "Duplicate columns with name 'id': In yaml_string.yml at: [2,4], [3,4]" in errors_str


def test_error_no_dataset(data_source_test_helper: DataSourceTestHelper):
    with pytest.raises(
        InvalidDatasetQualifiedNameException,
        match="Identifier must be a valid string and cannot be None"
    ):
        data_source_test_helper.assert_contract_error(
            contract_yaml_str=f"""
                columns:
                  - name: id
            """
        )


def test_valid_values_not_configured(data_source_test_helper: DataSourceTestHelper):
    error: str = data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            dataset: the_test_ds/a/b/TBLE
            columns:
              - name: id
                checks:
                  - invalid:
        """,
    )
    assert "Invalid check does not have any valid or invalid configurations" in error
