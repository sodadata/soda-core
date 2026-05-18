"""Handler for the ``soda data-standard verify`` CLI command.

The handler builds the data-source / Soda Cloud / yaml-source plumbing the
same way ``handle_verify_contract`` does, then dispatches to
``soda_data_standard.api.verify_data_standards`` via a lazy import so
soda-core itself never has a hard dependency on the extension package.
"""

from __future__ import annotations

from typing import Dict, Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import (
    InvalidArgumentException,
    InvalidDataSourceConfigurationException,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import CheckCollectionYamlSource, DataSourceYamlSource
from soda_core.contracts.api.verify_api import _create_datasource_yamls
from soda_core.contracts.impl.check_selector import CheckSelector


def handle_verify_data_standards(
    check_file_paths: list[str],
    data_source_file_paths: list[str],
    soda_cloud_file_path: Optional[str],
    variables: Optional[Dict[str, str]],
    publish: bool,
    verbose: bool,
    check_paths: Optional[list[str]],
    check_selectors: list[CheckSelector],
    diagnostics_warehouse_file_path: Optional[str],
) -> ExitCode:
    try:
        if not check_file_paths:
            raise InvalidArgumentException("At least one -c/--check file path is required for data-standard verify.")
        if not data_source_file_paths:
            raise InvalidArgumentException(
                "At least one -ds/--data-source file path is required for data-standard verify."
            )

        soda_cloud_client: Optional[SodaCloud] = None
        if soda_cloud_file_path:
            soda_cloud_client = SodaCloud.from_config(soda_cloud_file_path, variables)

        if publish and soda_cloud_client is None:
            raise InvalidArgumentException(
                "A Soda Cloud configuration file is required to use the -p/--publish argument. "
                "Please provide the '-sc/--soda-cloud' argument with a valid configuration file path."
            )

        # Build DataSourceImpl instances from the provided -ds yaml files.
        # We reuse ``_create_datasource_yamls`` from verify_api to keep the
        # same validation/error semantics as ``soda contract verify`` for
        # the local (non-agent) path.
        data_source_yamls: list[DataSourceYamlSource] = _create_datasource_yamls(
            data_source_file_paths=data_source_file_paths,
            use_agent=False,
        )
        all_data_source_impls: list[DataSourceImpl] = []
        for ds_yaml in data_source_yamls:
            ds_impl = DataSourceImpl.from_yaml_source(
                data_source_yaml_source=ds_yaml,
                provided_variable_values=variables,
            )
            if ds_impl is None:
                raise InvalidDataSourceConfigurationException(f"Failed to load data source from {ds_yaml.file_path}")
            all_data_source_impls.append(ds_impl)

        primary_data_source_impl: DataSourceImpl = all_data_source_impls[0]

        # Build yaml sources for each check file. Use the universal
        # CheckCollectionYamlSource alias (currently == YamlSource); the
        # engine reads ``kind:`` from each YAML and dispatches per-item.
        yaml_sources: list[CheckCollectionYamlSource] = [
            CheckCollectionYamlSource.from_file_path(p) for p in check_file_paths
        ]

        # Merge check_paths into check_selectors for parity with contract verify.
        merged_selectors: list[CheckSelector] = list(check_selectors) if check_selectors else []
        merged_selectors.extend(CheckSelector.from_check_paths(check_paths))

        # Lazy import — soda-core must not statically depend on the
        # soda-data-standard package. If it's not installed the user gets
        # a clear LOG_ERRORS exit and no traceback noise.
        try:
            from soda_data_standard.api import verify_data_standards
        except ImportError:
            soda_logger.error(
                "soda-data-standard is not installed. "
                "Install it to use 'soda data-standard verify' (e.g. `pip install soda-data-standard`)."
            )
            return ExitCode.LOG_ERRORS

        all_impls_by_name: Optional[dict[str, DataSourceImpl]] = (
            {ds.name: ds for ds in all_data_source_impls} if len(all_data_source_impls) > 1 else None
        )

        session_result = verify_data_standards(
            yaml_sources=yaml_sources,
            data_source_impl=primary_data_source_impl,
            soda_cloud_impl=soda_cloud_client,
            publish_results=publish,
            variables=variables,
            all_data_source_impls=all_impls_by_name,
            check_selectors=merged_selectors,
            dwh_data_source_file_path=diagnostics_warehouse_file_path,
        )

        return interpret_check_collection_session_result(session_result)

    except (InvalidArgumentException, InvalidDataSourceConfigurationException, Exception) as exc:
        soda_logger.exception(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS


def interpret_check_collection_session_result(session_result) -> ExitCode:
    """Map a ``CheckCollectionSessionResult`` to a CLI exit code.

    Mirrors ``interpret_contract_verification_result`` for the universal
    check-collection result (which doesn't expose aggregated ``has_errors``
    etc. as properties — we iterate the per-item results instead).
    """
    results = session_result.results if session_result is not None else []

    if any(r.sending_results_to_soda_cloud_failed for r in results):
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    if any(r.has_errors for r in results):
        return ExitCode.LOG_ERRORS
    if any(r.is_failed for r in results):
        return ExitCode.CHECK_FAILURES
    if any(r.is_warned for r in results):
        return ExitCode.CHECK_WARNINGS
    return ExitCode.OK
