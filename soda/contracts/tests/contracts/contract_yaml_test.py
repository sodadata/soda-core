import logging
from textwrap import dedent

from contracts.translation_test_helper import translate

logger = logging.getLogger(__name__)


def test_contract_data_type():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: id
            data_type: character varying
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                id: character varying
    """).strip()


def test_contract_not_null():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            checks:
              - type: no_missing_values
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - missing_count(cst_size) = 0
    """).strip()


def test_contract_missing_values():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            missing_values: ['N/A', 'No value']
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - missing_count(cst_size) = 0:
            missing values:
            - 'N/A'
            - 'No value'
    """).strip()


def test_contract_valid_values():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            valid_values: ['S', 'M', 'L']
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - invalid_count(cst_size) = 0:
            valid values:
            - 'S'
            - 'M'
            - 'L'
    """).strip()


def test_contract_missing_and_valid_values():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            missing_values: ['N/A', 'No value']
            valid_values: ['S', 'M', 'L']
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - missing_count(cst_size) = 0:
            missing values:
            - 'N/A'
            - 'No value'
        - invalid_count(cst_size) = 0:
            missing values:
            - 'N/A'
            - 'No value'
            valid values:
            - 'S'
            - 'M'
            - 'L'
    """).strip()


def test_contract_unique():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            unique: true
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - duplicate_count(cst_size) = 0
    """).strip()


def test_contract_row_count():
    assert translate(
        f"""
        dataset: CUSTOMERS
        checks:
          - type: row_count
            fail_when_not_between: [400, 500]
    """) == dedent(f"""
        checks for CUSTOMERS:
        - row_count between 400 and 500
    """).strip()


def test_contract_combination_schema_check_type_no_type():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: id
            data_type: character varying
          - name: cst_size
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                id: character varying
                cst_size:
    """).strip()


def test_contract_transformation_missing_values():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: cat
                missing_values: ['N/A', 'No value']
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    cat:
            - missing_count(cat) = 0:
                missing values:
                - 'N/A'
                - 'No value'
        """).strip()


def test_contract_transformation_valid_values():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: cat
                valid_values: ['a', 'b', 'c']
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    cat:
            - invalid_count(cat) = 0:
                valid values:
                - 'a'
                - 'b'
                - 'c'
        """).strip()


def test_contract_transformation_multi_valid():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: cat
                valid_values: ['a', 'b', 'c']
                valid_length: 1
                invalid_values: ['i', 'n', 'v']
                valid_min: 0
                valid_max: 10
                valid_regex: '.'
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    cat:
            - invalid_count(cat) = 0:
                valid values:
                - 'a'
                - 'b'
                - 'c'
                valid length: 1
                invalid values:
                - 'i'
                - 'n'
                - 'v'
                valid min: 0
                valid max: 10
                valid regex: '.'
        """).strip()


def test_contract_transformation_unique():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: id
                data_type: varchar
                unique: true
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    id: varchar
            - duplicate_count(id) = 0
        """).strip()


def test_contract_transformation_table_checks():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: cst_size
                data_type: integer

            checks:
              - type: avg
                column: cst_size
                fail_when_not_between: [400, 500]
              - type: min
                column: cst_size
                fail_when_less_than_or_equal: 10
              - type: max
                column: cst_size
                fail_when_greater_than_or_equal: 100
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    cst_size: integer
            - avg(cst_size) between 400 and 500
            - min(cst_size) > 10
            - max(cst_size) < 100
        """).strip()


def test_contract_transformation_reference():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: last_order_id
                checks:
                  - type: reference
                    dataset: ORDERS
                    column: id
                    samples_limit: 20
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    last_order_id:
            - values in (last_order_id) must exist in ORDERS (id):
                samples limit: 20
        """).strip()


def test_contract_transformation_ignore_other_keys():
    assert translate(
        f"""
            dataset: CUSTOMERS

            attributes:
              pii: very important

            anything: goes here

            columns:
              - name: last_order_id
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    last_order_id:
        """).strip()
