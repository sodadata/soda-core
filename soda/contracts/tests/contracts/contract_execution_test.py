import logging
from textwrap import dedent
from unittest import skip

from soda.contracts.data_contract_translator import DataContractTranslator
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


@skip
def test_contract_transformation_and_execution(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    source_yaml_string = dedent(
        f"""
        dataset: {table_name}

        columns:
          - name: id
            data_type: character varying
            unique: true
            attributes:

          - name: cst_size
            data_type: decimal
          - name: cst_size_txt
            valid_values: [1, 2, 3]
          - name: distance
            data_type: integer
          - name: pct
          - name: cat
          - name: country
            data_type: varchar
            not_null: true
          - name: zip
          - name: email
          - name: date_updated
            data_type: date
          - name: ts
          - name: ts_with_tz

        checks:
          - avg(distance) between 400 and 500
    """
    )

    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(source_yaml_string)

    expected_sodacl_yaml_str = dedent(
        f"""
        checks for {table_name}:
        - schema:
            fail:
              when mismatching columns:
                id: character varying
                cst_size: decimal
                cst_size_txt:
                distance: integer
                pct:
                cat:
                country: varchar
                zip:
                email:
                date_updated: date
                ts:
                ts_with_tz:
        - duplicate_count(id) = 0
        - invalid_count(cst_size_txt) = 0:
            valid format: decimal
        - missing_count(country) = 0
        - avg(distance) between 400 and 500
    """
    )

    assert sodacl_yaml_str.strip() == expected_sodacl_yaml_str.strip()

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(sodacl_yaml_str)
    scan.execute()

    scan.assert_all_checks_pass()
