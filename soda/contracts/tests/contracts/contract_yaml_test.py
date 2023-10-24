import logging
from textwrap import dedent

from contracts.data_contract_translator import DataContractTranslator

logger = logging.getLogger(__name__)


def test_contract_to_yaml_transformation():
    # project_root_dir = __file__[: -len("soda/contracts/tests/contracts/contract_yaml_test.py")]
    # contract_file_name = f"{project_root_dir}/soda/contracts/tests/contracts/contract_yaml.yml"
    # with open(contract_file_name) as f:
    #     source_yaml_string = f.read()

    source_yaml_string = dedent(
        """
        dataset: MARKETO_EVENTS

        schema:
          id:
            data_type: VARCHAR
            not_null: true
            valid_format: decimal
            unique: true
          name:
            data_type: VARCHAR
          size:
            data_type: FLOAT

        checks:
          - avg(size) between 20 and 50
    """
    )

    data_contract_parser = DataContractTranslator()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(source_yaml_string)

    expected_sodacl_yaml_str = dedent(
        """
        checks for MARKETO_EVENTS:
        - schema:
            fail:
              when columns not match:
                id: VARCHAR
                name: VARCHAR
                size: FLOAT
        - missing_count(id) = 0
        - duplicate_count(id) = 0
        - invalid_count(id) = 0:
            valid format: decimal
        - avg(size) between 20 and 50
    """
    )

    assert sodacl_yaml_str.strip() == expected_sodacl_yaml_str.strip()
