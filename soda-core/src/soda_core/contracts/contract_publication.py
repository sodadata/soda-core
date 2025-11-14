from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.contracts.contract_command_builder import ContractCommandBuilder
from soda_core.contracts.contract_verification import Contract, YamlFileContentInfo

logger: logging.Logger = soda_logger


class ContractPublicationBuilder(ContractCommandBuilder):
    def __init__(self, logs: Optional[Logs] = None):
        super().__init__(logs=logs)

    def build(self) -> ContractPublication:
        return ContractPublication(contract_publication_builder=self)


class ContractPublication:
    def __init__(self, contract_publication_builder: ContractPublicationBuilder) -> None:
        if not contract_publication_builder.soda_cloud and not contract_publication_builder.soda_cloud_yaml_source:
            logger.error(f"Cannot publish without a Soda Cloud configuration")

        from soda_core.contracts.impl.contract_publication_impl import (
            ContractPublicationImpl,
        )

        self.contract_publication_impl: ContractPublicationImpl = ContractPublicationImpl(
            contract_yaml_sources=contract_publication_builder.contract_yaml_sources,
            soda_cloud=contract_publication_builder.soda_cloud,
            soda_cloud_yaml_source=contract_publication_builder.soda_cloud_yaml_source,
            variables=contract_publication_builder.variables,
            logs=contract_publication_builder.logs,
        )

    @classmethod
    def builder(cls, logs: Optional[Logs] = None) -> ContractPublicationBuilder:
        return ContractPublicationBuilder(logs)

    def execute(self) -> ContractPublicationResultList:
        return self.contract_publication_impl.execute()


@dataclass
class Contract:
    data_source_name: str
    dataset_prefix: list[str]
    dataset_name: str
    soda_qualified_dataset_name: str
    source: YamlFileContentInfo
    dataset_id: str


@dataclass(frozen=True)
class ContractPublicationResultList:
    items: List[ContractPublicationResult]
    logs: Logs

    @property
    def has_errors(self) -> bool:
        return self.logs.has_errors

    def __len__(self):
        return len(self.items)

    def __getitem__(self, item):
        return self.items.__getitem__(item)


@dataclass(frozen=True)
class ContractPublicationResult:
    contract: Optional[Contract]
