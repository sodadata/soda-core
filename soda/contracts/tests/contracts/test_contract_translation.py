import logging
from textwrap import dedent

from contracts.helpers.schema_table import contracts_test_table
from contracts.helpers.test_connection import TestConnection
from contracts.helpers.translation_test_helper import translate
from soda.contracts.contract import ContractResult, SchemaCheckResult, CheckOutcome, Measurement

logger = logging.getLogger(__name__)


def test_contract_not_null():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            checks:
              - type: missing
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
            checks:
              - type: missing
                missing_values: ['N/A', 'No value']
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - missing_count(cst_size) = 0:
            missing values: ['N/A', 'No value']
    """).strip()


def test_contract_valid_values():
    assert translate(
        f"""
            dataset: CUSTOMERS
            columns:
              - name: cst_size
                checks:
                  - type: invalid
                    valid_values: ['S', 'M', 'L']
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    cst_size:
            - invalid_count(cst_size) = 0:
                valid values: ['S', 'M', 'L']
        """).strip()


def test_contract_missing_and_valid_values():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            checks:
              - type: missing
                missing_values: ['N/A', 'No value']
              - type: invalid
                valid_values: ['S', 'M', 'L']
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - missing_count(cst_size) = 0:
            missing values: ['N/A', 'No value']
        - invalid_count(cst_size) = 0:
            missing values: ['N/A', 'No value']
            valid values: ['S', 'M', 'L']
    """).strip()


def test_contract_unique():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: cst_size
            checks:
              - type: unique
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size:
        - duplicate_count(cst_size) = 0
    """).strip()


def test_contract_column_missing_values():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: cat
                checks:
                  - type: missing
                    missing_values: ['N/A', 'No value']
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    cat:
            - missing_count(cat) = 0:
                missing values: ['N/A', 'No value']
        """).strip()


def test_contract_column_valid_values():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: cat
                checks:
                  - type: invalid
                    valid_values: ['a', 'b', 'c']
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    cat:
            - invalid_count(cat) = 0:
                valid values: ['a', 'b', 'c']
        """).strip()


def test_contract_column_multi_valid():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: cat
                checks:
                  - type: invalid
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
                valid values: ['a', 'b', 'c']
                valid length: 1
                invalid values: ['i', 'n', 'v']
                valid min: 0
                valid max: 10
                valid regex: '.'
        """).strip()


def test_contract_column_unique():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: id
                data_type: varchar
                checks:
                  - type: unique
        """) == dedent(f"""
            checks for CUSTOMERS:
            - schema:
                fail:
                  when mismatching columns:
                    id: varchar
            - duplicate_count(id) = 0
        """).strip()


def test_contract_column_invalid_reference_check():
    assert translate(
        f"""
            dataset: CUSTOMERS

            columns:
              - name: last_order_id
                checks:
                  - type: invalid
                    valid_values_column:
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


def test_contract_dataset_row_count():
    assert translate(
        f"""
        dataset: CUSTOMERS
        columns:
          - name: id
        checks:
          - type: row_count
            fail_when_not_between: [400, 500]
    """) == dedent(f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                id:
        - row_count between 400 and 500
    """).strip()


def test_contract_ignore_other_keys():
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
