from __future__ import annotations

from typing import Optional, Type

from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import DataSourceYamlSource


class IContractGenerator:
    _registry: dict[str, Type[IContractGenerator]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        fully_qualified_name = cls.__module__ + "." + cls.__name__
        cls._registry[fully_qualified_name] = cls

    @classmethod
    def instance(cls, identifier: Optional[str] = None) -> IContractGenerator:
        if not cls._registry:
            raise NotImplementedError(f"No contract generator implementations registered.")
        if not identifier:
            identifier = next(iter(cls._registry))
        impl_cls = cls._registry[identifier]
        return impl_cls()

    def create_skeleton(
        self,
        data_source_yaml_source: DataSourceYamlSource,
        dataset_identifier: DatasetIdentifier,
        output_file_path: Optional[str],
        verbose: bool,
        soda_cloud: Optional[SodaCloud],
        use_agent: bool,
        generate_checks: bool = True,
    ):
        raise NotImplementedError("No implementation for create_skeleton() yet. Please install an appropriate plugin.")
