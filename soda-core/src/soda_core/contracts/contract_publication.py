from __future__ import annotations

from dataclasses import dataclass
from soda_core.common.logs import Logs, Emoticons
from soda_core.contracts.contract_command_builder import ContractCommandBuilder
from soda_core.contracts.contract_verification import Contract, YamlFileContentInfo
from typing import List, Optional


class ContractPublicationBuilder(ContractCommandBuilder):
    def __init__(self):
        super().__init__()

    def build(self) -> ContractPublication:
        return ContractPublication(contract_publication_builder=self)


class ContractPublication:
    def __init__(self, contract_publication_builder: ContractPublicationBuilder) -> None:
        if not contract_publication_builder.soda_cloud and not contract_publication_builder.soda_cloud_yaml_source:
            contract_publication_builder.logs.error(f"{Emoticons.POLICE_CAR_LIGHT} cannot publish without a Soda Cloud configuration")

        from soda_core.contracts.impl.contract_publication_impl import ContractPublicationImpl

        self.contract_publication_impl: ContractPublicationImpl = ContractPublicationImpl(
            contract_yaml_sources=contract_publication_builder.contract_yaml_sources,
            soda_cloud=contract_publication_builder.soda_cloud,
            soda_cloud_yaml_source=contract_publication_builder.soda_cloud_yaml_source,
            variables=contract_publication_builder.variables,
            logs=contract_publication_builder.logs,
        )

    @classmethod
    def builder(cls) -> ContractPublicationBuilder:
        return ContractPublicationBuilder()

    def execute(self) -> ContractPublicationResultList:
        return self.contract_publication_impl.execute()


@dataclass
class Contract:
    data_source_name: str
    dataset_prefix: list[str]
    dataset_name: str
    soda_qualified_dataset_name: str
    source: YamlFileContentInfo
    id: str

@dataclass(frozen=True)
class ContractPublicationResultList:
    items: List[ContractPublicationResult]
    logs: Logs

    def has_critical(self) -> bool:
        return any([r.has_critical() for r in self.items]) or self.logs.has_critical()

    def has_errors(self) -> bool:
        return any([r.has_errors() for r in self.items]) or self.logs.has_errors()

    def __len__(self):
        return len(self.items)

    def __getitem__(self, item):
        return self.items.__getitem__(item)


@dataclass(frozen=True)
class ContractPublicationResult:
    logs: Logs
    contract: Optional[Contract]

    def has_critical(self) -> bool:
        return self.logs.has_critical()

    def has_errors(self) -> bool:
        return self.logs.has_errors()


