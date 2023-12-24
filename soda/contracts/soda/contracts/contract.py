from __future__ import annotations

import logging
from textwrap import indent
from typing import List

import soda.common.logs as soda_common_logs
from scan import Scan
from soda.contracts.connection import Connection
from soda.contracts.contract_verification_result import ContractVerificationResult
from soda.contracts.impl.variable_resolver import VariableResolver
from soda.contracts.logs import Log, LogLevel, Logs, Location
from soda.contracts.soda_cloud import SodaCloud

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

    def verify(self, connection: Connection, soda_cloud: SodaCloud | None = None) -> ContractVerificationResult:
        # Resolve all the ${VARIABLES} in the contract based on system variables (os.environ)
        resolved_contract_yaml_str: str = VariableResolver.resolve(self.contract_yaml_str)

        # The contract verification result is mutable and will be collecting logs and results
        # during this method.
        contract_verification_result: ContractVerificationResult = ContractVerificationResult()

        # Translate the data contract into SodaCL
        # The contract_verification_result is passed (as Logs) to collect logs
        # noinspection PyProtectedMember
        data_contract_translator = connection._create_data_contract_translator(contract_verification_result)
        sodacl_yaml_str: str | None = data_contract_translator.translate_data_contract_yaml_str(
            resolved_contract_yaml_str
        )

        if data_contract_translator.has_errors():
            errors: List[Log] = data_contract_translator.get_errors()
            error_lines = "\n".join([str(error) for error in errors])
            logger.error(f"Data contract translation failed. Skipping execution. Errors: {error_lines}")
        else:
            # Logging or saving the SodaCL YAMl file will help with debugging potential scan execution issues
            logger.debug(f"SodaCL:\n{indent(sodacl_yaml_str, '  ')}")

            # This assumes the connection is a DataSourceConnection
            data_source = connection.data_source

            # Execute the contract SodaCL in a scan
            scan = Scan()
            try:
                scan.set_data_source_name(data_source.data_source_name)
                scan._data_source_manager.data_sources[data_source.data_source_name] = data_source
                scan.add_sodacl_yaml_str(sodacl_yaml_str)
                scan.execute()

            finally:
                LogMapper.transfer_logs(scan._logs.logs, contract_verification_result)

            # TODO extract results from scan and convert the into the contract verification result

        return contract_verification_result


class LogMapper:
    level_map = {
        soda_common_logs.LogLevel.ERROR: LogLevel.ERROR,
        soda_common_logs.LogLevel.WARNING: LogLevel.WARNING,
        soda_common_logs.LogLevel.INFO: LogLevel.INFO,
        soda_common_logs.LogLevel.DEBUG: LogLevel.DEBUG
    }

    @classmethod
    def transfer_logs(cls, scan_logs: List[soda_common_logs.Log], contract_logs: Logs):
        for log in scan_logs:
            contract_logs._log(Log(
                level=cls.translate_level(log.level),
                message=log.message,
                location=cls.translate_location(log.location),
                exception=log.exception
            ))

    @classmethod
    def translate_location(cls, location: soda_common_logs.Location):
        return Location(line=location.line, column=location.col) if location is not None else None

    @classmethod
    def translate_level(cls, level):
        return cls.level_map[level]
