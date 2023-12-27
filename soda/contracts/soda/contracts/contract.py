from __future__ import annotations

import logging
from textwrap import indent

from soda.contracts.connection import Connection
from soda.contracts.contract_result import ContractResult
from soda.contracts.impl.variable_resolver import VariableResolver
from soda.contracts.soda_cloud import SodaCloud
from soda.scan import Scan

logger = logging.getLogger(__name__)


class Contract:

    @classmethod
    def from_yaml_str(cls, contract_yaml_str: str) -> Contract:
        """
        Build a contract from a YAML string
        TODO document exceptions vs error response
        """
        return Contract(contract_yaml_str)

    @classmethod
    def from_yaml_file(cls, file_path: str) -> Contract:
        """
        Build a contract from a YAML file.
        TODO document exceptions vs error response
        """
        with open(file_path) as f:
            contract_yaml_str = f.read()
            return Contract(contract_yaml_str)

    def create_updated_contract(self, updates: dict) -> Contract:
        """
        Creates a new contract by applying the updates to this contract.
        Can be used for example to run a contract on a different schema or dataset name.
        """
        raise Exception("TODO")

    def __init__(self, contract_yaml_str: str, soda_cloud: SodaCloud | None = None):
        """
        Consider using Contract.create_from_yaml_str(contract_yaml_str) instead as that is more stable API.
        """
        self.contract_yaml_str: str = contract_yaml_str
        self.soda_cloud: SodaCloud = soda_cloud

    def verify(self, connection: Connection, soda_cloud: SodaCloud | None = None) -> ContractResult:
        # noinspection PyProtectedMember
        data_contract_translator = connection._create_data_contract_translator()
        scan = Scan()
        try:
            # Resolve all the ${VARIABLES} in the contract based on system variables (os.environ)
            resolved_contract_yaml_str: str = VariableResolver.resolve(self.contract_yaml_str)

            # Translate the data contract into SodaCL
            sodacl_yaml_str: str | None = data_contract_translator.translate_data_contract_yaml_str(
                resolved_contract_yaml_str
            )

            if not data_contract_translator.has_errors():
                # Logging or saving the SodaCL YAMl file will help with debugging potential scan execution issues
                logger.debug(f"SodaCL:\n{indent(sodacl_yaml_str, '  ')}")

                # This assumes the connection is a DataSourceConnection
                data_source = connection.data_source

                # Execute the contract SodaCL in a scan
                scan.set_data_source_name(data_source.data_source_name)
                # noinspection PyProtectedMember
                scan._data_source_manager.data_sources[data_source.data_source_name] = data_source
                scan.add_sodacl_yaml_str(sodacl_yaml_str)
                scan.execute()

        except Exception as e:
            logger.error(f"Data contract verification error: {e}", exc_info=True)

        # noinspection PyProtectedMember
        return ContractResult._from_scan_results(data_contract_translator.logs, scan)
