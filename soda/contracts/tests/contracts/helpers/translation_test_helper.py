from textwrap import dedent

from soda.contracts.impl.data_contract_translation import DataContractTranslation


def translate(contract_yaml_str) -> str:
    data_contract_parser = DataContractTranslation()
    sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(
        dedent(
            contract_yaml_str
        )
    )
    return sodacl_yaml_str.strip()
