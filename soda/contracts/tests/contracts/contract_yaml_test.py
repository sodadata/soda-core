import logging
from textwrap import dedent
from unittest import skip

from contracts.data_contract_translator import DataContractTranslator

logger = logging.getLogger(__name__)


def test_contract_transformation_basic():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: id
            data_type: character varying
          - name: cst_size
            data_type: decimal

        checks:
          - avg(cst_size) between 400 and 500
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                id: character varying
                cst_size: decimal
        - avg(cst_size) between 400 and 500
    """
        ).strip()
    )


def test_contract_transformation_no_data_type():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: id
            data_type: character varying
          - name: cst_size
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                id: character varying
                cst_size:
    """
        ).strip()
    )


def test_contract_transformation_not_nll():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: id
            data_type: varchar
            not_null: true
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                id: varchar
        - missing_count(id) = 0
    """
        ).strip()
    )


def test_contract_transformation_missing_values():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: cat
            missing_values: ['N/A', 'No value']
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cat:
        - missing_count(cat) = 0:
            missing values:
            - 'N/A'
            - 'No value'
    """
        ).strip()
    )


def test_contract_transformation_valid_values():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: cat
            valid_values: ['a', 'b', 'c']
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
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
    """
        ).strip()
    )


def test_contract_transformation_multi_valid():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
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
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
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
    """
        ).strip()
    )


def test_contract_transformation_unique():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: id
            data_type: varchar
            unique: true
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                id: varchar
        - duplicate_count(id) = 0
    """
        ).strip()
    )


def test_contract_transformation_table_checks():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: cst_size
            data_type: integer

        checks:
          - avg(cst_size) between 400 and 500
          - min(cst_size) > 10
          - max(cst_size) < 100
    """
        )
    )

    assert (
        sodacl_yaml_str.strip()
        == dedent(
            f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                cst_size: integer
        - avg(cst_size) between 400 and 500
        - min(cst_size) > 10
        - max(cst_size) < 100
    """
        ).strip()
    )


def test_contract_transformation_reference():
    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            f"""
        dataset: CUSTOMERS

        columns:
          - name: last_order_id
            reference:
              dataset: ORDERS
              column: id
              samples_limit: 20

        attributes:
          owner: johndoe
          age: 25 years
    """
        )
    )

    assert (
        sodacl_yaml_str.strip() == dedent(
            f"""
        checks for CUSTOMERS:
        - schema:
            fail:
              when mismatching columns:
                last_order_id:
        - values in (last_order_id) must exist in ORDERS (id):
            samples limit: 20

    """
        ).strip()
    )
