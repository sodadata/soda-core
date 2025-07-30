from typing import Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.file import File
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import VariableResolver, YamlObject, Yaml
from soda_core.contracts.contract_verification import ContractVerificationResult, SodaException
from soda_core.contracts.impl.diagnostics_configuration import DiagnosticsConfiguration


def verify_contract_locally(
    contract_file_path: Optional[str],
    contract_yaml_str: Optional[str],
    data_source_file_path: Optional[str],
    data_source_yaml_str: Optional[str],
    soda_cloud_file_path: Optional[str],
    soda_cloud_yaml_str: Optional[str],
    publish: bool,
    only_validate_without_execute: bool,
    variables: dict[str, str],
    data_timestamp: Optional[str],
    diagnostics_configuration: Optional[DiagnosticsConfiguration],
) -> ContractVerificationResult:
    """
    returns ContractVerificationResult or raises a SodaException
    """

    logs: Logs = Logs()

    data_source_impl: DataSourceImpl = _build_data_source_impl(
        data_source_file_path=data_source_file_path,
        data_source_yaml_str=data_source_yaml_str,
        logs=logs,
    )

    soda_cloud_impl: SodaCloud = _build_soda_cloud(
        soda_cloud_file_path=soda_cloud_file_path,
        soda_cloud_yaml_str=soda_cloud_yaml_str,
        logs=logs,
    )

    with data_source_impl:
        return _execute_locally_impl(
            data_source_impl=data_source_impl,
            soda_cloud_impl=soda_cloud_impl,
            contract_file_path=contract_file_path,
            contract_yaml_str=contract_yaml_str,
            publish=publish,
            only_validate_without_execute=only_validate_without_execute,
            variables=variables,
            data_timestamp=data_timestamp,
            diagnostics_configuration=diagnostics_configuration,
            logs=logs,
        )

def _execute_locally_impl(
    data_source_impl: DataSourceImpl,
    soda_cloud_impl: SodaCloud,
    contract_file_path: Optional[str],
    contract_yaml_str: Optional[str],
    publish: bool,
    only_validate_without_execute: bool,
    variables: dict[str, str],
    data_timestamp: Optional[str],
    diagnostics_configuration: Optional[DiagnosticsConfiguration],
    logs: Logs,
) -> ContractVerificationResult:



    contract_yaml: ContractYaml = ContractYaml.parse(
        contract_yaml_source=contract_yaml_source,
        provided_variable_values=variables,
        data_timestamp=data_timestamp,
    )

    contract_impl: ContractImpl = ContractImpl(
        contract_yaml=contract_yaml,
        only_validate_without_execute=only_validate_without_execute,
        data_timestamp=contract_yaml.data_timestamp,
        execution_timestamp=contract_yaml.execution_timestamp,
        data_source_impl=data_source_impl,
        soda_cloud=soda_cloud_impl,
        publish_results=publish,
        logs=logs,
    )

    return contract_impl.verify()


def _build_data_source_impl(
    data_source_file_path: Optional[str],
    data_source_yaml_str: Optional[str],
    variables: dict[str, str],
    logs: Logs
) -> DataSourceImpl:
    data_source_yaml_object: YamlObject = _parse_yaml_object(
        file_path=data_source_file_path,
        file_path_variable_name="data_source_file_path",
        yaml_str=data_source_yaml_str,
        yaml_str_variable_name="data_source_yaml_str",
        variables=variables,
        logs=logs,
    )

    return DataSourceImpl.parse(
        data_source_yaml_object=data_source_yaml_object,
        logs=logs
    )


def _build_soda_cloud(
    soda_cloud_file_path: Optional[str],
    soda_cloud_yaml_str: Optional[str],
    variables: dict[str, str],
    logs: Logs
) -> SodaCloud:
    soda_cloud_yaml_object: YamlObject = _parse_yaml_object(
        file_path=soda_cloud_file_path,
        file_path_variable_name="soda_cloud_file_path",
        yaml_str=soda_cloud_yaml_str,
        yaml_str_variable_name="soda_cloud_yaml_str",
        variables=variables,
        logs=logs,
    )

    return SodaCloud.parse(
        data_source_yaml_object=soda_cloud_yaml_object,
        logs=logs
    )


def _parse_yaml_object(
    file_path: Optional[str],
    file_path_variable_name: str,
    yaml_str: Optional[str],
    yaml_str_variable_name: str,
    variables: dict[str, str],
) -> YamlObject:
    if file_path is not None:
        if not isinstance(file_path, str):
            raise SodaException(
                f"Expected {file_path_variable_name} to be a string, but was {type(file_path).__name__}"
            )
        if yaml_str is not None:
            raise SodaException(
                f"Both {file_path_variable_name} and {yaml_str_variable_name} are provided. Only one expected."
            )

        yaml_str = File.read(file_path)

    if yaml_str is None:
        raise SodaException(
            f"No {file_path_variable_name} nor {yaml_str_variable_name} specified"
        )

    if not isinstance(yaml_str, str):
        raise SodaException(
            f"Expected {yaml_str_variable_name} to be a string, but was {type(yaml_str).__name__}"
        )

    resolved_yaml_str: str = VariableResolver.resolve(
        source_text=yaml_str, variable_values=variables, use_env_vars=True
    )

    return Yaml.parse(
        yaml_str=resolved_yaml_str,
    )
